-- Totales por partido
SELECT dp.nk_partido_id,
       SUM(CASE WHEN de.nombre=dp.equipo_local  THEN fa.puntos ELSE 0 END) AS puntos_local,
       SUM(CASE WHEN de.nombre=dp.equipo_visita THEN fa.puntos ELSE 0 END) AS puntos_visita
FROM dw.dim_partido dp
JOIN dw.fact_anotacion fa ON fa.sk_partido=dp.sk_partido
JOIN dw.dim_equipo de     ON de.sk_equipo=fa.sk_equipo
GROUP BY dp.nk_partido_id
ORDER BY dp.nk_partido_id DESC;

-- Conteo jugadores por equipo
SELECT e.nombre equipo, COUNT(*) jugadores
FROM dw.dim_jugador j
JOIN dw.dim_equipo e ON e.nombre=j.equipo_actual
GROUP BY e.nombre
ORDER BY 2 DESC;

-- Existencia de tiempo
SELECT COUNT(*) AS rows_tiempo FROM dw.dim_tiempo;
