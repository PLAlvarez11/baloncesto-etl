
-- Para diferenciar, usamos dw como esquema y tipo estrella
CREATE SCHEMA IF NOT EXISTS dw;


CREATE TABLE IF NOT EXISTS dw.dim_equipo (
  sk_equipo      BIGSERIAL PRIMARY KEY,
  nk_equipo_id   INT NOT NULL,          -- id_Equipo en OLTP
  nombre         VARCHAR(120) NOT NULL,
  ciudad         VARCHAR(120),
  codigo         VARCHAR(20),          
  activo         BOOLEAN DEFAULT TRUE,
  vigente_desde  TIMESTAMP DEFAULT NOW(),
  vigente_hasta  TIMESTAMP,
  UNIQUE (nk_equipo_id)
);

CREATE TABLE IF NOT EXISTS dw.dim_jugador (
  sk_jugador     BIGSERIAL PRIMARY KEY,
  nk_jugador_id  INT NOT NULL,          -- id_Jugador en OLTP
  nombre         VARCHAR(120) NOT NULL,
  apellido       VARCHAR(120),
  numero         INT,
  posicion       VARCHAR(50),
  edad           INT,
  estatura_cm    INT,
  nacionalidad   VARCHAR(80),
  equipo_actual  VARCHAR(120),
  vigente_desde  TIMESTAMP DEFAULT NOW(),
  vigente_hasta  TIMESTAMP,
  UNIQUE (nk_jugador_id)
);

CREATE TABLE IF NOT EXISTS dw.dim_partido (
  sk_partido     BIGSERIAL PRIMARY KEY,
  nk_partido_id  INT NOT NULL,          -- id_Partido en OLTP
  fecha_hora     TIMESTAMP NOT NULL,
  equipo_local   VARCHAR(120),
  equipo_visita  VARCHAR(120),
  localidad      VARCHAR(120),
  torneo         VARCHAR(120),
  temporada      VARCHAR(20),
  vigente_desde  TIMESTAMP DEFAULT NOW(),
  vigente_hasta  TIMESTAMP,
  UNIQUE (nk_partido_id)
);

CREATE TABLE IF NOT EXISTS dw.dim_localidad (
  sk_localidad    BIGSERIAL PRIMARY KEY,
  nk_localidad_id INT,
  nombre          VARCHAR(120) NOT NULL,
  ciudad          VARCHAR(120),
  pais            VARCHAR(80),
  UNIQUE (nk_localidad_id, nombre)
);

CREATE TABLE IF NOT EXISTS dw.dim_tiempo (
  sk_tiempo     BIGSERIAL PRIMARY KEY,
  fecha         DATE NOT NULL,
  anio          INT NOT NULL,
  mes           INT NOT NULL,
  dia           INT NOT NULL,
  trimestre     INT NOT NULL,
  hora          INT,
  minuto        INT,
  segundo       INT,
  nombre_mes    VARCHAR(15),
  nombre_dia    VARCHAR(15)
);

-- Indices asi mas rapida esa onda
CREATE INDEX IF NOT EXISTS idx_dim_tiempo_fecha ON dw.dim_tiempo(fecha);
CREATE INDEX IF NOT EXISTS idx_dim_tiempo_anio_mes ON dw.dim_tiempo(anio, mes);


-- Tablas para estadisticas y sus indeces creados
CREATE TABLE IF NOT EXISTS dw.fact_anotacion (
  sk_anotacion   BIGSERIAL PRIMARY KEY,
  sk_partido     BIGINT NOT NULL REFERENCES dw.dim_partido(sk_partido),
  sk_tiempo      BIGINT NOT NULL REFERENCES dw.dim_tiempo(sk_tiempo),
  sk_equipo      BIGINT NOT NULL REFERENCES dw.dim_equipo(sk_equipo),
  sk_jugador     BIGINT REFERENCES dw.dim_jugador(sk_jugador),
  cuarto         INT,             -- 1..4 (+OT)
  puntos         INT NOT NULL,    -- 1,2,3
  tipo_tiro      VARCHAR(30),     -- libre, doble, triple
  ts_evento      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_anotacion_partido_equipo ON dw.fact_anotacion(sk_partido, sk_equipo);
CREATE INDEX IF NOT EXISTS idx_fact_anotacion_jugador_partido ON dw.fact_anotacion(sk_jugador, sk_partido);
CREATE INDEX IF NOT EXISTS idx_fact_anotacion_tiempo ON dw.fact_anotacion(sk_tiempo);


CREATE TABLE IF NOT EXISTS dw.fact_falta (
  sk_falta       BIGSERIAL PRIMARY KEY,
  sk_partido     BIGINT NOT NULL REFERENCES dw.dim_partido(sk_partido),
  sk_tiempo      BIGINT NOT NULL REFERENCES dw.dim_tiempo(sk_tiempo),
  sk_equipo      BIGINT NOT NULL REFERENCES dw.dim_equipo(sk_equipo),
  sk_jugador     BIGINT REFERENCES dw.dim_jugador(sk_jugador),
  cuarto         INT,
  tipo_falta     VARCHAR(30),     -- personal, técnica, antideportiva...
  cantidad       INT NOT NULL DEFAULT 1,
  ts_evento      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_falta_partido_equipo ON dw.fact_falta(sk_partido, sk_equipo);
CREATE INDEX IF NOT EXISTS idx_fact_falta_jugador_partido ON dw.fact_falta(sk_jugador, sk_partido);
CREATE INDEX IF NOT EXISTS idx_fact_falta_tiempo ON dw.fact_falta(sk_tiempo);

--  Rol para ETL y rol de solo lectura para reportes
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'dw_etl') THEN
    CREATE ROLE dw_etl LOGIN PASSWORD 'dw_etl_2025!';
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'dw_report') THEN
    CREATE ROLE dw_report LOGIN PASSWORD 'dw_report_2025!';
  END IF;
END$$;

GRANT USAGE ON SCHEMA dw TO dw_etl, dw_report;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA dw TO dw_etl;
GRANT SELECT ON ALL TABLES IN SCHEMA dw TO dw_report;

ALTER DEFAULT PRIVILEGES IN SCHEMA dw GRANT SELECT ON TABLES TO dw_report;
ALTER DEFAULT PRIVILEGES IN SCHEMA dw GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO dw_etl;

-- Insercion de prueba
INSERT INTO dw.dim_tiempo (fecha, anio, mes, dia, trimestre, hora, minuto, segundo, nombre_mes, nombre_dia)
VALUES (
  CURRENT_DATE, EXTRACT(YEAR FROM CURRENT_DATE)::INT, EXTRACT(MONTH FROM CURRENT_DATE)::INT,
  EXTRACT(DAY FROM CURRENT_DATE)::INT, ((EXTRACT(MONTH FROM CURRENT_DATE)::INT - 1)/3 + 1)::INT,
  0, 0, 0, TO_CHAR(CURRENT_DATE, 'Mon'), TO_CHAR(CURRENT_DATE, 'Dy')
)
ON CONFLICT DO NOTHING;
