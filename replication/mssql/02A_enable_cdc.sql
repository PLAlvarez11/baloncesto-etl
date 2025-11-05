USE master;
GO
IF DB_ID('Tablero_DB') IS NULL
  THROW 51000, 'Base de datos Tablero_DB no existe.', 1;
GO

USE Tablero_DB;
GO

-- Habilitar CDC en la BD
IF EXISTS (SELECT 1 FROM sys.databases WHERE name='Tablero_DB' AND is_cdc_enabled=0)
  EXEC sys.sp_cdc_enable_db;
ELSE
  PRINT '[CDC] Ya habilitado en Tablero_DB';
GO

-- Rol lector CDC (opcional)
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name='cdc_reader')
BEGIN
  CREATE ROLE cdc_reader;
  GRANT SELECT ON SCHEMA::cdc TO cdc_reader;
END
GO

-- Habilitar CDC por tabla
DECLARE @t TABLE(schema_name SYSNAME, table_name SYSNAME);
INSERT INTO @t VALUES
('dbo','Localidades'),('dbo','Equipos'),('dbo','Jugadores'),
('dbo','Partidos'),('dbo','Cuartos'),('dbo','Anotacion'),('dbo','Falta');

DECLARE @s SYSNAME,@n SYSNAME,@sql NVARCHAR(MAX);
DECLARE c CURSOR FAST_FORWARD FOR SELECT schema_name,table_name FROM @t;
OPEN c; FETCH NEXT FROM c INTO @s,@n;
WHILE @@FETCH_STATUS=0
BEGIN
  IF NOT EXISTS (SELECT 1 FROM cdc.change_tables
    WHERE source_object_id = OBJECT_ID(QUOTENAME(@s)+'.'+QUOTENAME(@n)))
  BEGIN
    SET @sql = N'EXEC sys.sp_cdc_enable_table
      @source_schema=N''' + @s + ''',
      @source_name  =N''' + @n + ''',
      @role_name    =N''cdc_reader'',
      @supports_net_changes=1;';
    EXEC sp_executesql @sql;
    PRINT '[CDC] Habilitado: ' + @s + '.' + @n;
  END
  ELSE PRINT '[CDC] Ya estaba: ' + @s + '.' + @n;
  FETCH NEXT FROM c INTO @s,@n;
END
CLOSE c; DEALLOCATE c;
GO

-- Verificación
SELECT name,is_cdc_enabled FROM sys.databases WHERE name='Tablero_DB';
SELECT * FROM cdc.change_tables;

DECLARE @from_lsn BINARY(10)=sys.fn_cdc_get_min_lsn('dbo_Equipos');
DECLARE @to_lsn   BINARY(10)=sys.fn_cdc_get_max_lsn();
SELECT @from_lsn AS from_lsn, @to_lsn AS to_lsn;
