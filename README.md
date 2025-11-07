# ETL Stack (DW + Redis + Replicator)

## Preparación
docker network create analytics-net || true

## Levantar
docker compose --env-file .env.etl -f docker-compose.etl.yml up -d --build

## Verificar
docker ps
docker logs -f etl-replicator

# Arquitectura Final del Sistema de Reporterías ETL Distribuidas

## 1. Componentes Principales
- **Back-Tablero (.NET + SQL Server)**: sistema transaccional principal.
- **ETL-Stack (Postgres DW + Redis + Replicator)**: capa analítica.
- **Report-Service-PHP**: reportería en tiempo casi real con cache Redis.
- **VPS-Deploy (Caddy + TLS)**: proxy inverso seguro con Let’s Encrypt.
- **Observability-Stack (Prometheus + Grafana)**: monitoreo, métricas y alertas.

## 2. Flujo General de Datos
1. Eventos OLTP (SQL Server) → CDC → ETL Replicator.
2. Transformación y carga a PostgreSQL (DW).
3. Consultas analíticas y KPIs desde PHP.
4. Cacheo en Redis para velocidad.
5. Invalidation automática post-ETL o cierre de partido.

## 3. Redes y seguridad
- Red Docker: `analytics-net` (compartida).
- TLS: Caddy (Let's Encrypt) con HSTS, CSP, XFO, RP, PP.
- JWT: roles `report_reader`, `admin`.
- Logs rotados: `max-size: 10m`, `max-file: 3`.

## 4. Métricas y monitoreo
- `etl_replication_lag_seconds`: < 60s promedio.
- `reconcile_mismatch_total`: 0 en verificaciones diarias.
- Alertas: replicator caído, lag > 300s, disco > 85%.

## 5. Backup y recuperación
- SQL Server: `/srv/back-tablero/mssql/backups/*.bak`
- PostgreSQL: `/srv/etl-stack/backups/*.dump`
- Retención: 7 días.
