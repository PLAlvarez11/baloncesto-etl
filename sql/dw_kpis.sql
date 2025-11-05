--  anotadores por temporada 
CREATE OR REPLACE VIEW dw.v_kpi_top_anotadores AS
SELECT
  dp.temporada,
  dj.nk_jugador_id,
  dj.nombre,
  dj.apellido,
  SUM(fa.puntos) AS total_puntos
FROM dw.fact_anotacion fa
JOIN dw.dim_partido dp ON dp.sk_partido = fa.sk_partido
JOIN dw.dim_jugador dj ON dj.sk_jugador = fa.sk_jugador
GROUP BY dp.temporada, dj.nk_jugador_id, dj.nombre, dj.apellido
ORDER BY dp.temporada, total_puntos DESC;

--  Faltas por equipo
CREATE OR REPLACE VIEW dw.v_kpi_faltas_equipo_30d AS
SELECT
  de.nombre AS equipo,
  COUNT(*)  AS faltas_30d
FROM dw.fact_falta ff
JOIN dw.dim_equipo de ON de.sk_equipo = ff.sk_equipo
JOIN dw.dim_tiempo dt ON dt.sk_tiempo  = ff.sk_tiempo
WHERE dt.fecha >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY de.nombre
ORDER BY faltas_30d DESC;
