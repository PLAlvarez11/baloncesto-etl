import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

def mssql_engine() -> Engine:
    host = os.getenv("MSSQL_HOST","sqlserver")
    port = os.getenv("MSSQL_PORT","1433")
    db   = os.getenv("MSSQL_DB","Tablero_DB")
    user = os.getenv("MSSQL_USER","sa")
    pwd  = os.getenv("MSSQL_PASSWORD","")
    drv  = os.getenv("MSSQL_ODBC_DRIVER","ODBC Driver 18 for SQL Server")
    conn = f"mssql+pyodbc://{user}:{pwd}@{host}:{port}/{db}?driver={drv.replace(' ', '+')}&TrustServerCertificate=yes"
    return create_engine(conn, pool_pre_ping=True, fast_executemany=True)

def pg_engine(user_env="DW_USER", pwd_env="DW_PASSWORD") -> Engine:
    host = os.getenv("DW_HOST","postgres-dw")
    port = os.getenv("DW_PORT","5432")
    db   = os.getenv("DW_DB","BALONCESTO_DW")
    user = os.getenv(user_env,"dw_etl")
    pwd  = os.getenv(pwd_env,"dw_etl_2025!")
    conn = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(conn, pool_pre_ping=True)

def ensure_repl_tables(pg: Engine):
    with pg.begin() as cx:
        cx.execute(text("""
        CREATE TABLE IF NOT EXISTS dw.control_replication (
          id BIGSERIAL PRIMARY KEY,
          table_name   VARCHAR(100) NOT NULL,
          last_lsn     BYTEA,
          last_run_ts  TIMESTAMP DEFAULT NOW(),
          processed    BIGINT DEFAULT 0,
          note         TEXT
        );
        """))
        cx.execute(text("""
        CREATE TABLE IF NOT EXISTS dw.metrics_replication (
          id BIGSERIAL PRIMARY KEY,
          run_at       TIMESTAMP DEFAULT NOW(),
          table_name   VARCHAR(100),
          from_lsn     BYTEA,
          to_lsn       BYTEA,
          rows_read    BIGINT,
          rows_applied BIGINT,
          status       VARCHAR(20),
          message      TEXT
        );
        """))

def get_last_lsn(pg: Engine, table_name: str) -> bytes | None:
    with pg.connect() as cx:
        r = cx.execute(text("""
          SELECT last_lsn FROM dw.control_replication
          WHERE table_name=:t ORDER BY id DESC LIMIT 1
        """), {"t": table_name}).first()
        return bytes(r[0]) if r and r[0] else None

def upsert_last_lsn(pg: Engine, table_name: str, lsn: bytes, processed: int, note: str = "OK"):
    with pg.begin() as cx:
        cx.execute(text("""
          INSERT INTO dw.control_replication (table_name, last_lsn, processed, note)
          VALUES (:t, :lsn, :p, :n)
        """), {"t": table_name, "lsn": lsn, "p": processed, "n": note})

def add_metric(pg: Engine, table: str, from_lsn: bytes, to_lsn: bytes, rows_read:int, rows_applied:int, status:str, message:str):
    with pg.begin() as cx:
        cx.execute(text("""
          INSERT INTO dw.metrics_replication(table_name, from_lsn, to_lsn, rows_read, rows_applied, status, message)
          VALUES (:t, :f, :to, :rr, :ra, :s, :m)
        """), {"t": table, "f": from_lsn, "to": to_lsn, "rr": rows_read, "ra": rows_applied, "s": status, "m": message[:800]})
