CREATE SCHEMA IF NOT EXISTS dw;

CREATE OR REPLACE VIEW dw.v_equipo AS
SELECT
  de.sk_equipo,
  de.nk_equipo_id,
  de.nombre,
  de.ciudad,
  de.codigo,
  de.activo
FROM dw.dim_equipo de
WHERE de.activo IS TRUE;

CREATE OR REPLACE VIEW dw.v_jugadores_por_equipo AS
SELECT
  dj.sk_jugador,
  dj.nk_jugador_id,
  dj.nombre,
  dj.apellido,
  dj.numero,
  dj.posicion,
  dj.edad,
  dj.estatura_cm,
  dj.nacionalidad,
  dj.equipo_actual,
  de.nk_equipo_id AS nk_equipo_id
FROM dw.dim_jugador dj
JOIN dw.dim_equipo de ON de.nombre = dj.equipo_actual AND de.activo IS TRUE;

CREATE OR REPLACE VIEW dw.v_historial_partidos AS
WITH anot AS (
  SELECT
    dp.sk_partido,
    SUM(CASE WHEN de.nombre = dp.equipo_local  THEN fa.puntos ELSE 0 END) AS puntos_local,
    SUM(CASE WHEN de.nombre = dp.equipo_visita THEN fa.puntos ELSE 0 END) AS puntos_visita
  FROM dw.dim_partido dp
  LEFT JOIN dw.fact_anotacion fa ON fa.sk_partido = dp.sk_partido
  LEFT JOIN dw.dim_equipo de     ON de.sk_equipo   = fa.sk_equipo
  GROUP BY dp.sk_partido
)
SELECT
  dp.sk_partido,
  dp.nk_partido_id,
  dp.fecha_hora,
  dp.equipo_local,
  dp.equipo_visita,
  dp.localidad,
  dp.torneo,
  dp.temporada,
  COALESCE(a.puntos_local, 0)  AS puntos_local,
  COALESCE(a.puntos_visita, 0) AS puntos_visita
FROM dw.dim_partido dp
LEFT JOIN anot a ON a.sk_partido = dp.sk_partido
ORDER BY dp.fecha_hora DESC;

CREATE OR REPLACE VIEW dw.v_roster_por_partido AS
WITH part AS (
  SELECT dp.sk_partido, dp.nk_partido_id
  FROM dw.dim_partido dp
),
jug_a AS ( -- desde anotaciones
  SELECT DISTINCT
    p.nk_partido_id,
    dj.sk_jugador,
    dj.nk_jugador_id,
    dj.nombre,
    dj.apellido,
    de.nombre AS equipo
  FROM dw.fact_anotacion fa
  JOIN part p ON p.sk_partido = fa.sk_partido
  JOIN dw.dim_jugador dj ON dj.sk_jugador = fa.sk_jugador
  JOIN dw.dim_equipo  de ON de.sk_equipo  = fa.sk_equipo
),
jug_f AS ( -- desde faltas
  SELECT DISTINCT
    p.nk_partido_id,
    dj.sk_jugador,
    dj.nk_jugador_id,
    dj.nombre,
    dj.apellido,
    de.nombre AS equipo
  FROM dw.fact_falta ff
  JOIN part p ON p.sk_partido = ff.sk_partido
  JOIN dw.dim_jugador dj ON dj.sk_jugador = ff.sk_jugador
  JOIN dw.dim_equipo  de ON de.sk_equipo  = ff.sk_equipo
),
unioned AS (
  SELECT * FROM jug_a
  UNION
  SELECT * FROM jug_f
)
SELECT *
FROM unioned
ORDER BY nk_partido_id, equipo, apellido, nombre;
