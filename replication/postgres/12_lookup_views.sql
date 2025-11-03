-- Vistas de lookup NK→SK para uso rápido en inserts de hechos
CREATE OR REPLACE VIEW dw.v_equipo_sk AS
SELECT nk_equipo_id, sk_equipo FROM dw.dim_equipo;

CREATE OR REPLACE VIEW dw.v_jugador_sk AS
SELECT nk_jugador_id, sk_jugador FROM dw.dim_jugador;

CREATE OR REPLACE VIEW dw.v_partido_sk AS
SELECT nk_partido_id, sk_partido FROM dw.dim_partido;
