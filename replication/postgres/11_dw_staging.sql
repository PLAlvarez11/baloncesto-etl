
CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE IF NOT EXISTS stg.equipos (
  nk_equipo_id INT PRIMARY KEY,
  nombre       VARCHAR(120),
  ciudad       VARCHAR(120),
  updated_at   TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg.jugadores (
  nk_jugador_id INT PRIMARY KEY,
  nombre        VARCHAR(120),
  apellido      VARCHAR(120),
  numero        INT,
  posicion      VARCHAR(50),
  edad          INT,
  estatura_cm   INT,
  nacionalidad  VARCHAR(80),
  equipo_actual VARCHAR(120),
  updated_at    TIMESTAMP
);
