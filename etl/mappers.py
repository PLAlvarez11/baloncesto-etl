from sqlalchemy import text
from sqlalchemy.engine import Engine

UPSERT_DIM_EQUIPO = """
INSERT INTO dw.dim_equipo (nk_equipo_id, nombre, ciudad, codigo, activo)
VALUES (:nk_equipo_id, :nombre, :ciudad, :codigo, COALESCE(:activo, TRUE))
ON CONFLICT (nk_equipo_id)
DO UPDATE SET nombre = EXCLUDED.nombre,
              ciudad = EXCLUDED.ciudad,
              codigo = EXCLUDED.codigo,
              activo = COALESCE(EXCLUDED.activo, TRUE),
              vigente_desde = CASE
                   WHEN dw.dim_equipo.nombre IS DISTINCT FROM EXCLUDED.nombre
                     OR dw.dim_equipo.ciudad IS DISTINCT FROM EXCLUDED.ciudad
                     OR dw.dim_equipo.codigo IS DISTINCT FROM EXCLUDED.codigo
                   THEN NOW()
                   ELSE dw.dim_equipo.vigente_desde END;
"""

UPSERT_DIM_JUGADOR = """
INSERT INTO dw.dim_jugador
(nk_jugador_id, nombre, apellido, numero, posicion, edad, estatura_cm, nacionalidad, equipo_actual)
VALUES (:nk_jugador_id, :nombre, :apellido, :numero, :posicion, :edad, :estatura_cm, :nacionalidad, :equipo_actual)
ON CONFLICT (nk_jugador_id)
DO UPDATE SET nombre = EXCLUDED.nombre,
              apellido = EXCLUDED.apellido,
              numero = EXCLUDED.numero,
              posicion = EXCLUDED.posicion,
              edad = EXCLUDED.edad,
              estatura_cm = EXCLUDED.estatura_cm,
              nacionalidad = EXCLUDED.nacionalidad,
              equipo_actual = EXCLUDED.equipo_actual,
              vigente_desde = CASE
                WHEN dw.dim_jugador.nombre IS DISTINCT FROM EXCLUDED.nombre
                  OR dw.dim_jugador.apellido IS DISTINCT FROM EXCLUDED.apellido
                  OR dw.dim_jugador.numero IS DISTINCT FROM EXCLUDED.numero
                  OR dw.dim_jugador.posicion IS DISTINCT FROM EXCLUDED.posicion
                  OR dw.dim_jugador.equipo_actual IS DISTINCT FROM EXCLUDED.equipo_actual
                THEN NOW()
                ELSE dw.dim_jugador.vigente_desde END;
"""

UPSERT_DIM_PARTIDO = """
INSERT INTO dw.dim_partido
(nk_partido_id, fecha_hora, equipo_local, equipo_visita, localidad, torneo, temporada)
VALUES (:nk_partido_id, :fecha_hora, :equipo_local, :equipo_visita, :localidad, :torneo, :temporada)
ON CONFLICT (nk_partido_id)
DO UPDATE SET fecha_hora = EXCLUDED.fecha_hora,
              equipo_local = EXCLUDED.equipo_local,
              equipo_visita = EXCLUDED.equipo_visita,
              localidad = EXCLUDED.localidad,
              torneo = EXCLUDED.torneo,
              temporada = EXCLUDED.temporada;
"""

UPSERT_DIM_TIEMPO = """
INSERT INTO dw.dim_tiempo (fecha, anio, mes, dia, trimestre, hora, minuto, segundo, nombre_mes, nombre_dia)
VALUES (DATE(:ts), EXTRACT(YEAR FROM :ts)::INT, EXTRACT(MONTH FROM :ts)::INT,
        EXTRACT(DAY FROM :ts)::INT, ((EXTRACT(MONTH FROM :ts)::INT - 1)/3 + 1)::INT,
        EXTRACT(HOUR FROM :ts)::INT, EXTRACT(MINUTE FROM :ts)::INT, EXTRACT(SECOND FROM :ts)::INT,
        TO_CHAR(:ts, 'Mon'), TO_CHAR(:ts, 'Dy'))
ON CONFLICT DO NOTHING;
"""

def upsert_many(pg: Engine, sql: str, rows: list[dict]):
    if not rows:
        return
    with pg.begin() as cx:
        cx.execute(text(sql), rows)
