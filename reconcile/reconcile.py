import os, time, datetime as dt
from sqlalchemy import create_engine, text
from prometheus_client import start_http_server, Gauge

MISMATCH = Gauge('reconcile_mismatch_total', 'Total tablas con diferencias (conteos)')

def mssql_engine():
    host=os.getenv("MSSQL_HOST","sqlserver"); port=os.getenv("MSSQL_PORT","1433")
    db=os.getenv("MSSQL_DB","Tablero_DB"); user=os.getenv("MSSQL_USER","sa")
    pwd=os.getenv("MSSQL_PASSWORD",""); drv=os.getenv("MSSQL_ODBC_DRIVER","ODBC Driver 18 for SQL Server")
    conn=f"mssql+pyodbc://{user}:{pwd}@{host}:{port}/{db}?driver={drv.replace(' ','+')}&TrustServerCertificate=yes"
    return create_engine(conn, pool_pre_ping=True)

def pg_engine():
    host=os.getenv("DW_HOST","postgres-dw"); port=os.getenv("DW_PORT","5432")
    db=os.getenv("DW_DB","BALONCESTO_DW"); user=os.getenv("DW_USER","dw_etl"); pwd=os.getenv("DW_PASSWORD","dw_etl_2025!")
    return create_engine(f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}", pool_pre_ping=True)

CHECKS = [
  ("Equipos",    "SELECT COUNT(*) FROM dbo.Equipos",    "SELECT COUNT(*) FROM dw.dim_equipo"),
  ("Jugadores",  "SELECT COUNT(*) FROM dbo.Jugadores",  "SELECT COUNT(*) FROM dw.dim_jugador"),
  ("Partidos",   "SELECT COUNT(*) FROM dbo.Partidos",   "SELECT COUNT(*) FROM dw.dim_partido"),
  ("Anotacion",  "SELECT COUNT(*) FROM dbo.Anotacion",  "SELECT COUNT(*) FROM dw.fact_anotacion"),
  ("Falta",      "SELECT COUNT(*) FROM dbo.Falta",      "SELECT COUNT(*) FROM dw.fact_falta"),
]

def run_once(mssql, pg):
    mismatches = 0
    with mssql.connect() as ms, pg.connect() as p:
        for name, q1, q2 in CHECKS:
            c1 = ms.execute(text(q1)).scalar() or 0
            c2 = p.execute(text(q2)).scalar() or 0
            if c1 != c2:
                mismatches += 1
                print(f"[RECONCILE] {name}: OLTP={c1} DW={c2}")
            else:
                print(f"[RECONCILE] {name}: OK ({c1})")
    MISMATCH.set(mismatches)

if __name__ == "__main__":
    start_http_server(8010)  
    ms = mssql_engine(); p = pg_engine()
    while True:
        now = dt.datetime.utcnow()
        if now.hour == 2 and now.minute in (30,31):
            try:
                run_once(ms, p)
            except Exception as e:
                print("[RECONCILE] ERROR:", e)
            time.sleep(120)
        time.sleep(20)
