# ETL Stack (DW + Redis + Replicator)

## Preparación
docker network create analytics-net || true

## Levantar
docker compose --env-file .env.etl -f docker-compose.etl.yml up -d --build

## Verificar
docker ps
docker logs -f etl-replicator
