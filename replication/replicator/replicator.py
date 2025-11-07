import os,time,sys
from datetime import datetime
from sqlalchemy import text
from db import mssql_engine, pg_engine, ensure_repl_tables, get_last_lsn, upsert_last_lsn, add_metric
from cdc_reader import lsn_bounds, read_changes
from transform_load import (
  upsert_equipo, upsert_localidad, upsert_jugador, upsert_partido,
  ensure_dim_tiempo, insert_fact_anotacion, insert_fact_falta
)

from prometheus_client import start_http_server, Counter, Gauge

MET_ROWS_READ  = Counter('etl_rep_rows_read_total',  'Rows read from CDC',    ['table'])
MET_ROWS_APPL  = Counter('etl_rep_rows_applied_total','Rows applied to DW',    ['table'])
MET_ERRORS     = Counter('etl_rep_errors_total',      'Errors by table',       ['table'])
MET_LAG_SEC    = Gauge('etl_replication_lag_seconds', 'Replication lag seconds (CDC max LSN time to now)')

LOG_LEVEL=os.getenv("REPL_LOG_LEVEL","INFO").upper()
BATCH_SEC=int(os.getenv("REPL_BATCH_SECONDS","15"))
TABLES=[t.strip() for t in os.getenv("REPL_TABLES","").split(",") if t.strip()]

def log(msg):
    if LOG_LEVEL in ("INFO","DEBUG"):
        print(f"[{datetime.utcnow().isoformat()}] {msg}", flush=True)

def lsn_to_time(mssql, lsn: bytes):
    with mssql.connect() as cx:
        return cx.execute(text("SELECT sys.fn_cdc_map_lsn_to_time(:lsn)"), {"lsn": lsn}).scalar()

def main():
    mssql=mssql_engine()
    pg=pg_engine()
    ensure_repl_tables(pg)
    if not TABLES:
        print("REPL_TABLES vacío", file=sys.stderr); sys.exit(1)

    start_http_server(8000)

    dims=[t for t in ["Localidades","Equipos","Jugadores","Partidos","Cuartos"] if t in TABLES]
    facts=[t for t in ["Anotacion","Falta"] if t in TABLES]

    while True:
        for group in [dims,facts]:
            for tbl in group:
                try:
                    last_lsn=get_last_lsn(pg,tbl)
                    global_from,global_to=lsn_bounds(mssql,tbl)
                    from_lsn=last_lsn or global_from
                    to_lsn=global_to

                    changes=read_changes(mssql,tbl,from_lsn,to_lsn)
                    rows_read=len(changes); rows_applied=0
                    if rows_read:
                        log(f"{tbl}: {rows_read} cambios")
                        MET_ROWS_READ.labels(tbl).inc(rows_read)

                    if tbl=="Localidades":
                        for ch in changes:
                            if ch.get("__$operation") in (2,4):
                                upsert_localidad(pg,ch); rows_applied+=1

                    elif tbl=="Equipos":
                        for ch in changes:
                            if ch.get("__$operation") in (2,4):
                                upsert_equipo(pg,ch); rows_applied+=1

                    elif tbl=="Jugadores":
                        for ch in changes:
                            if ch.get("__$operation") in (2,4):
                                with pg.connect() as cx:
                                    rs=cx.execute(text("SELECT nombre FROM dw.dim_equipo WHERE nk_equipo_id=:nk"),
                                                  {"nk":ch.get("id_Equipo")}).first()
                                    equipo_nombre=rs[0] if rs else None
                                upsert_jugador(pg,ch,equipo_nombre); rows_applied+=1

                    elif tbl=="Partidos":
                        for ch in changes:
                            if ch.get("__$operation") in (2,4):
                                with pg.connect() as cx:
                                    local=visita=localidad=None
                                    if ch.get("id_Local") is not None:
                                        r=cx.execute(text("SELECT nombre FROM dw.dim_equipo WHERE nk_equipo_id=:nk"),
                                                     {"nk":ch.get("id_Local")}).first()
                                        local=r[0] if r else None
                                    if ch.get("id_Visitante") is not None:
                                        r=cx.execute(text("SELECT nombre FROM dw.dim_equipo WHERE nk_equipo_id=:nk"),
                                                     {"nk":ch.get("id_Visitante")}).first()
                                        visita=r[0] if r else None
                                    if ch.get("id_Localidad") is not None:
                                        r=cx.execute(text("SELECT nombre FROM dw.dim_localidad WHERE nk_localidad_id=:nk"),
                                                     {"nk":ch.get("id_Localidad")}).first()
                                        localidad=r[0] if r else None
                                upsert_partido(pg,ch,{"local":local,"visita":visita,"localidad":localidad})
                                rows_applied+=1

                    elif tbl=="Cuartos":
                        pass

                    elif tbl=="Anotacion":
                        for ch in changes:
                            if ch.get("__$operation") in (2,4):
                                ts=ch.get("FechaHora") or ch.get("ts_evento")
                                ensure_dim_tiempo(pg,ts)
                                payload={
                                  "nk_partido_id": ch.get("id_partido"),
                                  "nk_equipo_id":  ch.get("id_Equipo") or ch.get("id_equipo"),
                                  "nk_jugador_id": ch.get("id_jugador"),
                                  "cuarto":        ch.get("id_cuarto") or ch.get("nro_cuarto"),
                                  "puntos":        ch.get("total_anotaciones"),
                                  "ts_evento":     ts
                                }
                                insert_fact_anotacion(pg,payload); rows_applied+=1

                    elif tbl=="Falta":
                        for ch in changes:
                            if ch.get("__$operation") in (2,4):
                                ts=ch.get("FechaHora") or ch.get("ts_evento")
                                ensure_dim_tiempo(pg,ts)
                                payload={
                                  "nk_partido_id": ch.get("id_partido"),
                                  "nk_equipo_id":  ch.get("id_Equipo") or ch.get("id_equipo"),
                                  "nk_jugador_id": ch.get("id_jugador"),
                                  "cuarto":        ch.get("id_cuarto") or ch.get("nro_cuarto"),
                                  "cantidad":      ch.get("total_falta"),
                                  "ts_evento":     ts
                                }
                                insert_fact_falta(pg,payload); rows_applied+=1

                    MET_ROWS_APPL.labels(tbl).inc(rows_applied)

                    to_time = lsn_to_time(mssql, to_lsn)
                    if to_time:
                        from datetime import timezone
                        import datetime as dt
                        lag = (dt.datetime.now(tz=timezone.utc) - to_time).total_seconds()
                        MET_LAG_SEC.set(max(lag, 0))

                    upsert_last_lsn(pg,tbl,to_lsn,rows_applied)
                    add_metric(pg,tbl,from_lsn,to_lsn,rows_read,rows_applied,"OK","applied")

                except Exception as e:
                    MET_ERRORS.labels(tbl).inc()
                    add_metric(pg,tbl,b"",b"",0,0,"ERROR",str(e))
                    print(f"[ERROR] {tbl}: {e}", file=sys.stderr)

        time.sleep(BATCH_SEC)


if __name__=="__main__":
    main()
