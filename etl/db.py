import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

def mssql_engine() -> Engine:
    host = os.getenv("MSSQL_HOST", "sqlserver")
    port = os.getenv("MSSQL_PORT", "1433")
    db   = os.getenv("MSSQL_DB", "Tablero_DB")
    user = os.getenv("MSSQL_USER", "sa")
    pwd  = os.getenv("MSSQL_PASSWORD", "")
    odbc = os.getenv("MSSQL_ODBC_DRIVER", "ODBC Driver 18 for SQL Server")
    # TrustServerCertificate=yes  si es necesario descomentar 
    conn_str = (
        f"mssql+pyodbc://{user}:{pwd}@{host}:{port}/{db}"
        f"?driver={odbc.replace(' ', '+')}&TrustServerCertificate=yes"
    )
    return create_engine(conn_str, pool_pre_ping=True, fast_executemany=True)

def pg_engine(user_env="DW_USER", pwd_env="DW_PASSWORD") -> Engine:
    host = os.getenv("DW_HOST", "postgres-dw")
    port = os.getenv("DW_PORT", "5432")
    db   = os.getenv("DW_DB", "BALONCESTO_DW")
    user = os.getenv(user_env, "dw_etl")
    pwd  = os.getenv(pwd_env, "dw_etl_2025!")
    conn_str = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(conn_str, pool_pre_ping=True)

def ensure_control_tables(pg: Engine):
    with pg.begin() as cx:
        cx.execute(text("""
        CREATE TABLE IF NOT EXISTS dw.control_etl (
          id            BIGSERIAL PRIMARY KEY,
          proceso       VARCHAR(100) NOT NULL,
          last_run_ts   TIMESTAMP,
          note          TEXT,
          created_at    TIMESTAMP DEFAULT NOW()
        );
        """))
        # seed watermark si no existe fila de proceso principal
        cx.execute(text("""
        INSERT INTO dw.control_etl (proceso, last_run_ts, note)
        SELECT 'main', NULL, 'Inicial'
        WHERE NOT EXISTS (SELECT 1 FROM dw.control_etl WHERE proceso='main');
        """))

def get_last_watermark(pg: Engine) -> str | None:
    with pg.connect() as cx:
        rs = cx.execute(text("""
            SELECT last_run_ts FROM dw.control_etl
            WHERE proceso='main' ORDER BY id DESC LIMIT 1
        """)).first()
        return rs[0].isoformat() if rs and rs[0] else None

def update_watermark(pg: Engine):
    with pg.begin() as cx:
        cx.execute(text("""
            UPDATE dw.control_etl
            SET last_run_ts = NOW(), note = 'OK'
            WHERE proceso='main';
        """))
