from sqlalchemy import text
from sqlalchemy.engine import Engine

# Upserts dimensiones 
UPSERT_DIM_EQUIPO = """
INSERT INTO dw.dim_equipo(nk_equipo_id, nombre, ciudad, codigo, activo)
VALUES (:nk, :nombre, :ciudad, NULL, TRUE)
ON CONFLICT (nk_equipo_id)
DO UPDATE SET nombre=EXCLUDED.nombre,
              ciudad=EXCLUDED.ciudad,
              activo=TRUE;
"""

UPSERT_DIM_LOCALIDAD = """
INSERT INTO dw.dim_localidad(nk_localidad_id, nombre, ciudad, pais)
VALUES (:nk, :nombre, NULL, NULL)
ON CONFLICT (nk_localidad_id, nombre) DO NOTHING;
"""

UPSERT_DIM_JUGADOR = """
INSERT INTO dw.dim_jugador(nk_jugador_id, nombre, apellido, numero, posicion, edad, estatura_cm, nacionalidad, equipo_actual)
VALUES (:nk, :nombre, :apellido, :numero, NULL, :edad, :estatura_cm, :nacionalidad, :equipo_actual)
ON CONFLICT (nk_jugador_id)
DO UPDATE SET nombre=EXCLUDED.nombre,
              apellido=EXCLUDED.apellido,
              numero=EXCLUDED.numero,
              edad=EXCLUDED.edad,
              estatura_cm=EXCLUDED.estatura_cm,
              nacionalidad=EXCLUDED.nacionalidad,
              equipo_actual=EXCLUDED.equipo_actual;
"""

UPSERT_DIM_PARTIDO = """
INSERT INTO dw.dim_partido(nk_partido_id, fecha_hora, equipo_local, equipo_visita, localidad, torneo, temporada)
VALUES (:nk, :fecha_hora, :equipo_local, :equipo_visita, :localidad, NULL, NULL)
ON CONFLICT (nk_partido_id)
DO UPDATE SET fecha_hora=EXCLUDED.fecha_hora,
              equipo_local=EXCLUDED.equipo_local,
              equipo_visita=EXCLUDED.equipo_visita,
              localidad=EXCLUDED.localidad;
"""

UPSERT_DIM_TIEMPO_BY_TS = """
INSERT INTO dw.dim_tiempo (fecha, anio, mes, dia, trimestre, hora, minuto, segundo, nombre_mes, nombre_dia)
VALUES (DATE(:ts), EXTRACT(YEAR FROM :ts)::INT, EXTRACT(MONTH FROM :ts)::INT, EXTRACT(DAY FROM :ts)::INT,
        ((EXTRACT(MONTH FROM :ts)::INT - 1)/3 + 1)::INT, EXTRACT(HOUR FROM :ts)::INT,
        EXTRACT(MINUTE FROM :ts)::INT, EXTRACT(SECOND FROM :ts)::INT, TO_CHAR(:ts,'Mon'), TO_CHAR(:ts,'Dy'))
ON CONFLICT DO NOTHING;
"""

#  Inserts hechos (resolviendo SK por NK) 
INSERT_FACT_ANOTACION = """
WITH s AS (
  SELECT :nk_partido_id::INT nk_partido_id,
         :nk_equipo_id::INT  nk_equipo_id,
         :nk_jugador_id::INT nk_jugador_id,
         :cuarto::INT        cuarto,
         :puntos::INT        puntos,
         :ts_evento::TIMESTAMP ts_evento
)
INSERT INTO dw.fact_anotacion(sk_partido, sk_tiempo, sk_equipo, sk_jugador, cuarto, puntos, tipo_tiro, ts_evento)
SELECT dp.sk_partido, dt.sk_tiempo, de.sk_equipo, dj.sk_jugador, s.cuarto, s.puntos, NULL, s.ts_evento
FROM s
JOIN dw.dim_partido dp ON dp.nk_partido_id = s.nk_partido_id
JOIN dw.dim_equipo de  ON de.nk_equipo_id  = s.nk_equipo_id
LEFT JOIN dw.dim_jugador dj ON dj.nk_jugador_id = s.nk_jugador_id
JOIN dw.dim_tiempo dt  ON dt.fecha = DATE(s.ts_evento)
                        AND dt.hora = EXTRACT(HOUR FROM s.ts_evento)
                        AND dt.minuto = EXTRACT(MINUTE FROM s.ts_evento)
                        AND dt.segundo = EXTRACT(SECOND FROM s.ts_evento);
"""

INSERT_FACT_FALTA = """
WITH s AS (
  SELECT :nk_partido_id::INT nk_partido_id,
         :nk_equipo_id::INT  nk_equipo_id,
         :nk_jugador_id::INT nk_jugador_id,
         :cuarto::INT        cuarto,
         :cantidad::INT      cantidad,
         :ts_evento::TIMESTAMP ts_evento
)
INSERT INTO dw.fact_falta(sk_partido, sk_tiempo, sk_equipo, sk_jugador, cuarto, tipo_falta, cantidad, ts_evento)
SELECT dp.sk_partido, dt.sk_tiempo, de.sk_equipo, dj.sk_jugador, s.cuarto, NULL, s.cantidad, s.ts_evento
FROM s
JOIN dw.dim_partido dp ON dp.nk_partido_id = s.nk_partido_id
JOIN dw.dim_equipo de  ON de.nk_equipo_id  = s.nk_equipo_id
LEFT JOIN dw.dim_jugador dj ON dj.nk_jugador_id = s.nk_jugador_id
JOIN dw.dim_tiempo dt  ON dt.fecha = DATE(s.ts_evento)
                        AND dt.hora = EXTRACT(HOUR FROM s.ts_evento)
                        AND dt.minuto = EXTRACT(MINUTE FROM s.ts_evento)
                        AND dt.segundo = EXTRACT(SECOND FROM s.ts_evento);
"""

def upsert_equipo(pg: Engine, row: dict):
    with pg.begin() as cx:
        cx.execute(text(UPSERT_DIM_EQUIPO), {
            "nk": row.get("id_Equipo") or row.get("id_equipo"),
            "nombre": row.get("Nombre") or row.get("nombre"),
            "ciudad": row.get("Ciudad") or row.get("ciudad")
        })

def upsert_localidad(pg: Engine, row: dict):
    with pg.begin() as cx:
        cx.execute(text(UPSERT_DIM_LOCALIDAD), {
            "nk": row.get("id_Localidad"),
            "nombre": row.get("Nombre") or row.get("nombre")
        })

def upsert_jugador(pg: Engine, row: dict, equipo_nombre: str | None):
    with pg.begin() as cx:
        cx.execute(text(UPSERT_DIM_JUGADOR), {
            "nk": row.get("id_Jugador") or row.get("id_jugador"),
            "nombre": row.get("Nombre") or row.get("nombre"),
            "apellido": row.get("Apellido") or row.get("apellido"),
            "numero": row.get("Numero_jugador") or row.get("numero"),
            "edad": row.get("edad"),
            "estatura_cm": row.get("altura") or row.get("estatura_cm"),
            "nacionalidad": row.get("nacionalidad"),
            "equipo_actual": equipo_nombre
        })

def upsert_partido(pg: Engine, row: dict, nombres: dict):
    with pg.begin() as cx:
        cx.execute(text(UPSERT_DIM_PARTIDO), {
            "nk": row.get("id_Partido") or row.get("id_partido"),
            "fecha_hora": row.get("FechaHora") or row.get("fecha_hora"),
            "equipo_local": nombres.get("local"),
            "equipo_visita": nombres.get("visita"),
            "localidad": nombres.get("localidad")
        })

def ensure_dim_tiempo(pg: Engine, ts):
    with pg.begin() as cx:
        cx.execute(text(UPSERT_DIM_TIEMPO_BY_TS), {"ts": ts})

def insert_fact_anotacion(pg: Engine, payload: dict):
    with pg.begin() as cx:
        cx.execute(text(INSERT_FACT_ANOTACION), payload)

def insert_fact_falta(pg: Engine, payload: dict):
    with pg.begin() as cx:
        cx.execute(text(INSERT_FACT_FALTA), payload)
