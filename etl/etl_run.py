import os, argparse, datetime as dt
import pandas as pd
from sqlalchemy import text
from db import mssql_engine, pg_engine, ensure_control_tables, get_last_watermark, update_watermark
from mappers import upsert_many, UPSERT_DIM_EQUIPO, UPSERT_DIM_JUGADOR, UPSERT_DIM_PARTIDO, UPSERT_DIM_TIEMPO

def parse_args():
    ap = argparse.ArgumentParser(description="ETL bajo demanda: SQL Server -> Postgres DW")
    ap.add_argument("--desde", type=str, help="YYYY-MM-DD (override watermark)")
    ap.add_argument("--hasta", type=str, help="YYYY-MM-DD (por defecto: hoy)")
    return ap.parse_args()

def date_bounds(args, last_wm):
    mode = os.getenv("ETL_MODE", "watermark").lower()
    if args.desde:
        desde = dt.datetime.fromisoformat(args.desde)
    elif mode == "watermark" and last_wm:
        desde = dt.datetime.fromisoformat(last_wm)
    else:
        lookback = int(os.getenv("ETL_DEFAULT_LOOKBACK_DAYS", "30"))
        desde = dt.datetime.now() - dt.timedelta(days=lookback)

    hasta = dt.datetime.fromisoformat(args.hasta) if args.hasta else dt.datetime.now()
    return desde, hasta

def extract_df(sql, eng, params=None):
    return pd.read_sql_query(text(sql), eng, params=params or {})

def load_dim_equipo(pg, df_eq):
    rows = []
    for _, r in df_eq.iterrows():
        rows.append({
            "nk_equipo_id": int(r["id_equipo"]),
            "nombre": r.get("nombre") or r.get("Nombre") or "",
            "ciudad": r.get("ciudad") or r.get("Ciudad"),
            "codigo": r.get("codigo") or None,
            "activo": True
        })
    upsert_many(pg, UPSERT_DIM_EQUIPO, rows)

def load_dim_jugador(pg, df_j):
    rows = []
    for _, r in df_j.iterrows():
        rows.append({
            "nk_jugador_id": int(r["id_jugador"]),
            "nombre": r.get("nombre") or r.get("Nombre") or "",
            "apellido": r.get("apellido") or r.get("Apellido"),
            "numero": int(r["numero"]) if pd.notna(r.get("numero")) else None,
            "posicion": r.get("posicion"),
            "edad": int(r["edad"]) if pd.notna(r.get("edad")) else None,
            "estatura_cm": int(r["estatura_cm"]) if pd.notna(r.get("estatura_cm")) else None,
            "nacionalidad": r.get("nacionalidad"),
            "equipo_actual": r.get("equipo_nombre") or r.get("equipo")
        })
    upsert_many(pg, UPSERT_DIM_JUGADOR, rows)

def load_dim_partido(pg, df_p):
    rows = []
    for _, r in df_p.iterrows():
        rows.append({
            "nk_partido_id": int(r["id_partido"]),
            "fecha_hora": r["fecha_hora"],
            "equipo_local": r.get("local_nombre"),
            "equipo_visita": r.get("visita_nombre"),
            "localidad": r.get("localidad"),
            "torneo": r.get("torneo"),
            "temporada": r.get("temporada"),
        })
    upsert_many(pg, UPSERT_DIM_PARTIDO, rows)

def ensure_dim_tiempo(pg, ts_series: pd.Series):
    unique_ts = sorted({pd.Timestamp(x).to_pydatetime().replace(microsecond=0)
                        for x in ts_series.dropna().tolist()})
    rows = [{"ts": t} for t in unique_ts]
    upsert_many(pg, UPSERT_DIM_TIEMPO, rows)

def load_fact_anotacion(pg, df):
    if df.empty: 
        return
    with pg.begin() as cx:
        cx.execute(text("""
        WITH src AS (
          SELECT 
            CAST(:nk_partido_id AS INT)    AS nk_partido_id,
            CAST(:nk_equipo_id  AS INT)    AS nk_equipo_id,
            CAST(:nk_jugador_id AS INT)    AS nk_jugador_id,
            CAST(:cuarto        AS INT)    AS cuarto,
            CAST(:puntos        AS INT)    AS puntos,
            CAST(:ts_evento     AS TIMESTAMP) AS ts_evento,
            :tipo_tiro          AS tipo_tiro
        )
        INSERT INTO dw.fact_anotacion (sk_partido, sk_tiempo, sk_equipo, sk_jugador, cuarto, puntos, tipo_tiro, ts_evento)
        SELECT dp.sk_partido,
               dt.sk_tiempo,
               de.sk_equipo,
               dj.sk_jugador,
               s.cuarto, s.puntos, s.tipo_tiro, s.ts_evento
        FROM src s
        JOIN dw.dim_partido dp   ON dp.nk_partido_id = s.nk_partido_id
        JOIN dw.dim_equipo de    ON de.nk_equipo_id  = s.nk_equipo_id
        LEFT JOIN dw.dim_jugador dj ON dj.nk_jugador_id = s.nk_jugador_id
        JOIN dw.dim_tiempo dt    ON dt.fecha = DATE(s.ts_evento)
                                  AND dt.hora = EXTRACT(HOUR FROM s.ts_evento)
                                  AND dt.minuto = EXTRACT(MINUTE FROM s.ts_evento)
                                  AND dt.segundo = EXTRACT(SECOND FROM s.ts_evento)
        """), df.to_dict(orient="records"))

def load_fact_falta(pg, df):
    if df.empty:
        return
    with pg.begin() as cx:
        cx.execute(text("""
        WITH src AS (
          SELECT 
            CAST(:nk_partido_id AS INT)    AS nk_partido_id,
            CAST(:nk_equipo_id  AS INT)    AS nk_equipo_id,
            CAST(:nk_jugador_id AS INT)    AS nk_jugador_id,
            CAST(:cuarto        AS INT)    AS cuarto,
            CAST(:cantidad      AS INT)    AS cantidad,
            CAST(:ts_evento     AS TIMESTAMP) AS ts_evento,
            :tipo_falta         AS tipo_falta
        )
        INSERT INTO dw.fact_falta (sk_partido, sk_tiempo, sk_equipo, sk_jugador, cuarto, tipo_falta, cantidad, ts_evento)
        SELECT dp.sk_partido,
               dt.sk_tiempo,
               de.sk_equipo,
               dj.sk_jugador,
               s.cuarto, s.tipo_falta, s.cantidad, s.ts_evento
        FROM src s
        JOIN dw.dim_partido dp   ON dp.nk_partido_id = s.nk_partido_id
        JOIN dw.dim_equipo de    ON de.nk_equipo_id  = s.nk_equipo_id
        LEFT JOIN dw.dim_jugador dj ON dj.nk_jugador_id = s.nk_jugador_id
        JOIN dw.dim_tiempo dt    ON dt.fecha = DATE(s.ts_evento)
                                  AND dt.hora = EXTRACT(HOUR FROM s.ts_evento)
                                  AND dt.minuto = EXTRACT(MINUTE FROM s.ts_evento)
                                  AND dt.segundo = EXTRACT(SECOND FROM s.ts_evento)
        """), df.to_dict(orient="records"))

def main():
    args = parse_args()
    mssql = mssql_engine()
    pg = pg_engine()
    ensure_control_tables(pg)

    last_wm = get_last_watermark(pg)
    desde, hasta = date_bounds(args, last_wm)
    print(f"[ETL] Ventana: {desde} → {hasta}")

    schema = os.getenv("SOURCE_SCHEMA", "dbo")

    # Ajusta campos según las tablas reales en OLTP
    q_equipos = f"""
      SELECT e.id_equipo, e.Nombre AS nombre, l.Nombre AS ciudad, NULL AS codigo
      FROM {schema}.Equipos e
      LEFT JOIN {schema}.Localidades l ON l.id_Localidad = e.id_Localidad
      WHERE e.FechaActualiza BETWEEN :desde AND :hasta OR e.FechaCrea BETWEEN :desde AND :hasta
         OR :last_wm IS NULL
    """
    q_jugadores = f"""
      SELECT j.id_jugador, j.Nombre AS nombre, j.Apellido AS apellido, j.Numero_jugador AS numero,
             NULL AS posicion, j.edad, j.altura AS estatura_cm, j.nacionalidad,
             e.Nombre AS equipo_nombre, j.id_Equipo
      FROM {schema}.Jugadores j
      JOIN {schema}.Equipos e ON e.id_Equipo = j.id_Equipo
      WHERE j.FechaActualiza BETWEEN :desde AND :hasta OR j.FechaCrea BETWEEN :desde AND :hasta
         OR :last_wm IS NULL
    """
    q_partidos = f"""
      SELECT p.id_partido, p.FechaHora AS fecha_hora,
             eL.Nombre AS local_nombre, eV.Nombre AS visita_nombre,
             l.Nombre AS localidad, NULL AS torneo, NULL AS temporada
      FROM {schema}.Partidos p
      LEFT JOIN {schema}.Equipos eL ON eL.id_Equipo = p.id_Local
      LEFT JOIN {schema}.Equipos eV ON eV.id_Equipo = p.id_Visitante
      LEFT JOIN {schema}.Localidades l ON l.id_Localidad = p.id_Localidad
      WHERE p.FechaHora BETWEEN :desde AND :hasta
         OR :last_wm IS NULL
    """
    # Cambia nombres de campos según tus tablas reales de anotaciones y las faltas
    q_anotacion = f"""
      SELECT a.id_partido AS nk_partido_id,
             COALESCE(c.id_equipo, p.id_Local) AS nk_equipo_id,
             a.id_jugador AS nk_jugador_id,
             c.nro_cuarto AS cuarto,
             a.total_anotaciones AS puntos,
             p.FechaHora AS ts_evento,
             NULL AS tipo_tiro
      FROM {schema}.Anotacion a
      JOIN {schema}.Partidos p ON p.id_Partido = a.id_partido
      LEFT JOIN {schema}.Cuartos c ON c.id_partido = a.id_partido AND c.id_cuarto = a.id_cuarto
      WHERE a.FechaActualiza BETWEEN :desde AND :hasta OR a.FechaCrea BETWEEN :desde AND :hasta
         OR :last_wm IS NULL
    """
    q_falta = f"""
      SELECT f.id_partido AS nk_partido_id,
             COALESCE(c.id_equipo, p.id_Local) AS nk_equipo_id,
             f.id_jugador AS nk_jugador_id,
             c.nro_cuarto AS cuarto,
             f.total_falta AS cantidad,
             p.FechaHora AS ts_evento,
             NULL AS tipo_falta
      FROM {schema}.Falta f
      JOIN {schema}.Partidos p ON p.id_Partido = f.id_partido
      LEFT JOIN {schema}.Cuartos c ON c.id_partido = f.id_partido AND c.id_cuarto = f.id_cuarto
      WHERE f.FechaActualiza BETWEEN :desde AND :hasta OR f.FechaCrea BETWEEN :desde AND :hasta
         OR :last_wm IS NULL
    """

    params = {"desde": desde, "hasta": hasta, "last_wm": last_wm}

    df_eq = extract_df(q_equipos, mssql, params)
    df_j  = extract_df(q_jugadores, mssql, params)
    df_p  = extract_df(q_partidos, mssql, params)
    df_a  = extract_df(q_anotacion, mssql, params)
    df_f  = extract_df(q_falta, mssql, params)

    print(f"[EXTRACT] equipos={len(df_eq)} jugadores={len(df_j)} partidos={len(df_p)} anotaciones={len(df_a)} faltas={len(df_f)}")

    load_dim_equipo(pg, df_eq)
    load_dim_jugador(pg, df_j)
    load_dim_partido(pg, df_p)

    ts_series = pd.concat([
        df_p["fecha_hora"] if "fecha_hora" in df_p else pd.Series([], dtype="datetime64[ns]"),
        df_a["ts_evento"] if "ts_evento" in df_a else pd.Series([], dtype="datetime64[ns]"),
        df_f["ts_evento"] if "ts_evento" in df_f else pd.Series([], dtype="datetime64[ns]")
    ], ignore_index=True)
    ensure_dim_tiempo(pg, ts_series)

    for col in ("nk_partido_id","nk_equipo_id","nk_jugador_id","cuarto","puntos","cantidad"):
        if col in df_a.columns and col in ("nk_partido_id","nk_equipo_id","nk_jugador_id","cuarto","puntos"):
            df_a[col] = pd.to_numeric(df_a[col], errors="coerce")
        if col in df_f.columns and col in ("nk_partido_id","nk_equipo_id","nk_jugador_id","cuarto","cantidad"):
            df_f[col] = pd.to_numeric(df_f[col], errors="coerce")

    load_fact_anotacion(pg, df_a.to_dict(orient="records"))
    load_fact_falta(pg, df_f.to_dict(orient="records"))

    update_watermark(pg)
    print("[ETL] OK")

if __name__ == "__main__":
    main()
