USE Tablero_DB;
GO

-- Localidades
IF COL_LENGTH('dbo.Localidades','FechaCrea') IS NULL
  ALTER TABLE dbo.Localidades ADD FechaCrea DATETIME2(3) NOT NULL
    CONSTRAINT DF_Localidades_FechaCrea DEFAULT (SYSUTCDATETIME());
IF COL_LENGTH('dbo.Localidades','FechaActualiza') IS NULL
  ALTER TABLE dbo.Localidades ADD FechaActualiza DATETIME2(3) NOT NULL
    CONSTRAINT DF_Localidades_FechaActualiza DEFAULT (SYSUTCDATETIME());
IF OBJECT_ID('dbo.tr_Localidades_AuditUpdate','TR') IS NOT NULL DROP TRIGGER dbo.tr_Localidades_AuditUpdate;
GO
CREATE TRIGGER dbo.tr_Localidades_AuditUpdate ON dbo.Localidades AFTER UPDATE AS
BEGIN SET NOCOUNT ON;
  UPDATE l SET FechaActualiza=SYSUTCDATETIME() FROM dbo.Localidades l JOIN inserted i ON i.id_Localidad=l.id_Localidad;
END
GO

-- Equipos
IF COL_LENGTH('dbo.Equipos','FechaCrea') IS NULL
  ALTER TABLE dbo.Equipos ADD FechaCrea DATETIME2(3) NOT NULL
    CONSTRAINT DF_Equipos_FechaCrea DEFAULT (SYSUTCDATETIME());
IF COL_LENGTH('dbo.Equipos','FechaActualiza') IS NULL
  ALTER TABLE dbo.Equipos ADD FechaActualiza DATETIME2(3) NOT NULL
    CONSTRAINT DF_Equipos_FechaActualiza DEFAULT (SYSUTCDATETIME());
IF OBJECT_ID('dbo.tr_Equipos_AuditUpdate','TR') IS NOT NULL DROP TRIGGER dbo.tr_Equipos_AuditUpdate;
GO
CREATE TRIGGER dbo.tr_Equipos_AuditUpdate ON dbo.Equipos AFTER UPDATE AS
BEGIN SET NOCOUNT ON;
  UPDATE e SET FechaActualiza=SYSUTCDATETIME() FROM dbo.Equipos e JOIN inserted i ON i.id_Equipo=e.id_Equipo;
END
GO

-- Jugadores
IF COL_LENGTH('dbo.Jugadores','FechaCrea') IS NULL
  ALTER TABLE dbo.Jugadores ADD FechaCrea DATETIME2(3) NOT NULL
    CONSTRAINT DF_Jugadores_FechaCrea DEFAULT (SYSUTCDATETIME());
IF COL_LENGTH('dbo.Jugadores','FechaActualiza') IS NULL
  ALTER TABLE dbo.Jugadores ADD FechaActualiza DATETIME2(3) NOT NULL
    CONSTRAINT DF_Jugadores_FechaActualiza DEFAULT (SYSUTCDATETIME());
IF OBJECT_ID('dbo.tr_Jugadores_AuditUpdate','TR') IS NOT NULL DROP TRIGGER dbo.tr_Jugadores_AuditUpdate;
GO
CREATE TRIGGER dbo.tr_Jugadores_AuditUpdate ON dbo.Jugadores AFTER UPDATE AS
BEGIN SET NOCOUNT ON;
  UPDATE j SET FechaActualiza=SYSUTCDATETIME() FROM dbo.Jugadores j JOIN inserted i ON i.id_Jugador=j.id_Jugador;
END
GO

-- Partidos
IF COL_LENGTH('dbo.Partidos','FechaCrea') IS NULL
  ALTER TABLE dbo.Partidos ADD FechaCrea DATETIME2(3) NOT NULL
    CONSTRAINT DF_Partidos_FechaCrea DEFAULT (SYSUTCDATETIME());
IF COL_LENGTH('dbo.Partidos','FechaActualiza') IS NULL
  ALTER TABLE dbo.Partidos ADD FechaActualiza DATETIME2(3) NOT NULL
    CONSTRAINT DF_Partidos_FechaActualiza DEFAULT (SYSUTCDATETIME());
IF OBJECT_ID('dbo.tr_Partidos_AuditUpdate','TR') IS NOT NULL DROP TRIGGER dbo.tr_Partidos_AuditUpdate;
GO
CREATE TRIGGER dbo.tr_Partidos_AuditUpdate ON dbo.Partidos AFTER UPDATE AS
BEGIN SET NOCOUNT ON;
  UPDATE p SET FechaActualiza=SYSUTCDATETIME() FROM dbo.Partidos p JOIN inserted i ON i.id_Partido=p.id_Partido;
END
GO

-- Cuartos
IF COL_LENGTH('dbo.Cuartos','FechaCrea') IS NULL
  ALTER TABLE dbo.Cuartos ADD FechaCrea DATETIME2(3) NOT NULL
    CONSTRAINT DF_Cuartos_FechaCrea DEFAULT (SYSUTCDATETIME());
IF COL_LENGTH('dbo.Cuartos','FechaActualiza') IS NULL
  ALTER TABLE dbo.Cuartos ADD FechaActualiza DATETIME2(3) NOT NULL
    CONSTRAINT DF_Cuartos_FechaActualiza DEFAULT (SYSUTCDATETIME());
IF OBJECT_ID('dbo.tr_Cuartos_AuditUpdate','TR') IS NOT NULL DROP TRIGGER dbo.tr_Cuartos_AuditUpdate;
GO
CREATE TRIGGER dbo.tr_Cuartos_AuditUpdate ON dbo.Cuartos AFTER UPDATE AS
BEGIN SET NOCOUNT ON;
  UPDATE c SET FechaActualiza=SYSUTCDATETIME()
  FROM dbo.Cuartos c JOIN inserted i ON i.id_cuarto=c.id_cuarto AND i.id_partido=c.id_partido;
END
GO

-- Anotacion
IF COL_LENGTH('dbo.Anotacion','FechaCrea') IS NULL
  ALTER TABLE dbo.Anotacion ADD FechaCrea DATETIME2(3) NOT NULL
    CONSTRAINT DF_Anotacion_FechaCrea DEFAULT (SYSUTCDATETIME());
IF COL_LENGTH('dbo.Anotacion','FechaActualiza') IS NULL
  ALTER TABLE dbo.Anotacion ADD FechaActualiza DATETIME2(3) NOT NULL
    CONSTRAINT DF_Anotacion_FechaActualiza DEFAULT (SYSUTCDATETIME());
IF OBJECT_ID('dbo.tr_Anotacion_AuditUpdate','TR') IS NOT NULL DROP TRIGGER dbo.tr_Anotacion_AuditUpdate;
GO
CREATE TRIGGER dbo.tr_Anotacion_AuditUpdate ON dbo.Anotacion AFTER UPDATE AS
BEGIN SET NOCOUNT ON;
  UPDATE a SET FechaActualiza=SYSUTCDATETIME() FROM dbo.Anotacion a JOIN inserted i ON i.id=a.id;
END
GO

-- Falta
IF COL_LENGTH('dbo.Falta','FechaCrea') IS NULL
  ALTER TABLE dbo.Falta ADD FechaCrea DATETIME2(3) NOT NULL
    CONSTRAINT DF_Falta_FechaCrea DEFAULT (SYSUTCDATETIME());
IF COL_LENGTH('dbo.Falta','FechaActualiza') IS NULL
  ALTER TABLE dbo.Falta ADD FechaActualiza DATETIME2(3) NOT NULL
    CONSTRAINT DF_Falta_FechaActualiza DEFAULT (SYSUTCDATETIME());
IF OBJECT_ID('dbo.tr_Falta_AuditUpdate','TR') IS NOT NULL DROP TRIGGER dbo.tr_Falta_AuditUpdate;
GO
CREATE TRIGGER dbo.tr_Falta_AuditUpdate ON dbo.Falta AFTER UPDATE AS
BEGIN SET NOCOUNT ON;
  UPDATE f SET FechaActualiza=SYSUTCDATETIME() FROM dbo.Falta f JOIN inserted i ON i.id=f.id;
END
GO
