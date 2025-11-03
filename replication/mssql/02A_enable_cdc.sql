
USE master;
GO

IF DB_ID('Tablero_DB') IS NULL
  THROW 51000, 'Base de datos Tablero_DB no existe.', 1;
GO

USE Tablero_DB;
GO

-- Habilitar CDC a nivel BD (si no está)
IF EXISTS (SELECT 1 FROM sys.databases WHERE name='Tablero_DB' AND is_cdc_enabled=0)
BEGIN
  EXEC sys.sp_cdc_enable_db;
  PRINT '[CDC] Habilitado a nivel de base de datos.';
END
ELSE
BEGIN
  PRINT '[CDC] Ya estaba habilitado a nivel de base de datos.';
END
GO

-- Rol lector CDC 
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name='cdc_reader')
BEGIN
  CREATE ROLE cdc_reader;
  GRANT SELECT ON SCHEMA::cdc TO cdc_reader;
END
GO

-- Habilitar CDC por tabla 
DECLARE @tbls TABLE (schema_name SYSNAME, table_name SYSNAME);
INSERT INTO @tbls(schema_name, table_name)
VALUES
('dbo','Localidades'),
('dbo','Equipos'),
('dbo','Jugadores'),
('dbo','Partidos'),
('dbo','Cuartos'),
('dbo','Anotacion'),
('dbo','Falta');

DECLARE @s SYSNAME, @t SYSNAME, @sql NVARCHAR(MAX);
DECLARE cur CURSOR FAST_FORWARD FOR SELECT schema_name, table_name FROM @tbls;
OPEN cur;
FETCH NEXT FROM cur INTO @s, @t;
WHILE @@FETCH_STATUS = 0
BEGIN
    IF NOT EXISTS (
      SELECT 1 FROM cdc.change_tables
      WHERE source_object_id = OBJECT_ID(QUOTENAME(@s)+'.'+QUOTENAME(@t))
    )
    BEGIN
        SET @sql = N'EXEC sys.sp_cdc_enable_table
          @source_schema = N''' + @s + ''',
          @source_name   = N''' + @t + ''',
          @role_name     = N''cdc_reader'',
          @supports_net_changes = 1;';
        EXEC sp_executesql @sql;
        PRINT '[CDC] Tabla habilitada: ' + @s + '.' + @t;
    END
    ELSE
    BEGIN
        PRINT '[CDC] Tabla ya habilitada: ' + @s + '.' + @t;
    END
    FETCH NEXT FROM cur INTO @s, @t;
END
CLOSE cur; DEALLOCATE cur;
GO

--  Consultas de verificación CDC
SELECT name AS cdc_capture_job, is_cdc_enabled FROM sys.databases WHERE name='Tablero_DB';

SELECT * FROM cdc.change_tables;

-- LSN bounds
DECLARE @from_lsn BINARY(10) = sys.fn_cdc_get_min_lsn('dbo_Equipos');
DECLARE @to_lsn   BINARY(10) = sys.fn_cdc_get_max_lsn();

SELECT @from_lsn AS from_lsn, @to_lsn AS to_lsn;
