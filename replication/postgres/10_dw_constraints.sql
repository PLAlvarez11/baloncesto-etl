-- Esto se ejecuta en postgre
ALTER TABLE dw.dim_equipo    ADD CONSTRAINT uq_dim_equipo_nk UNIQUE (nk_equipo_id);
ALTER TABLE dw.dim_jugador   ADD CONSTRAINT uq_dim_jugador_nk UNIQUE (nk_jugador_id);
ALTER TABLE dw.dim_partido   ADD CONSTRAINT uq_dim_partido_nk UNIQUE (nk_partido_id);
ALTER TABLE dw.dim_localidad ADD CONSTRAINT uq_dim_localidad UNIQUE (nk_localidad_id, nombre);
