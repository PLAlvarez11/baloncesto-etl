from sqlalchemy import text
from sqlalchemy.engine import Engine
from typing import Tuple

CDC_FN_ALL={
  "Localidades":"cdc.fn_cdc_get_all_changes_dbo_Localidades",
  "Equipos":"cdc.fn_cdc_get_all_changes_dbo_Equipos",
  "Jugadores":"cdc.fn_cdc_get_all_changes_dbo_Jugadores",
  "Partidos":"cdc.fn_cdc_get_all_changes_dbo_Partidos",
  "Cuartos":"cdc.fn_cdc_get_all_changes_dbo_Cuartos",
  "Anotacion":"cdc.fn_cdc_get_all_changes_dbo_Anotacion",
  "Falta":"cdc.fn_cdc_get_all_changes_dbo_Falta",
}

def lsn_bounds(mssql: Engine, table: str) -> Tuple[bytes, bytes]:
    with mssql.connect() as cx:
        from_lsn=cx.execute(text("SELECT sys.fn_cdc_get_min_lsn('dbo_Equipos')")).scalar()
        to_lsn=cx.execute(text("SELECT sys.fn_cdc_get_max_lsn()")).scalar()
        return from_lsn,to_lsn

def read_changes(mssql: Engine, table: str, from_lsn: bytes, to_lsn: bytes):
    fn=CDC_FN_ALL[table]
    sql=f"SELECT * FROM {fn}(:f,:t,'all')"
    with mssql.connect() as cx:
        rs=cx.execute(text(sql),{"f":from_lsn,"t":to_lsn})
        cols=rs.keys()
        return [dict(zip(cols,row)) for row in rs.fetchall()]
