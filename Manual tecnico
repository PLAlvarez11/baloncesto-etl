### Manual Tecnico
### Estructura del Repositorio
etl-stack/
├── etl/
│   ├── Dockerfile
│   ├── db.py
│   ├── etl_run.py
│   ├── mappers.py
│   ├── requirements.txt
│
├── reconcile/
│   ├── Dockerfile
│   ├── reconcile.py
│   └── requirements.txt
│
├── replication/
│   ├── contracts/
│   │   ├── catalog.yaml
│   │   └── conflict-matrix.md
│   │
│   ├── mssql/
│   │   ├── 01_ensure_audit_columns.sql
│   │   └── 02A_enable_cdc.sql
│   │
│   ├── postgres/
│   │   ├── 10_dw_constraints.sql
│   │   ├── 11_dw_staging.sql
│   │   └── 12_lookup_views.sql
│   │
│   ├── replicator/
│   │   ├── Dockerfile
│   │   ├── cdc_reader.py
│   │   ├── db.py
│   │   ├── replicator.py
│   │   ├── transform_load.py
│   │   └── requirements.txt
│   │
│   └── sql/
│       ├── cache/
│       │   ├── invalidate_on_etl.py
│       │   └── invalidate_on_match_close.py
│       ├── checks_dw.sql
│       ├── dw_kpis.sql
│       ├── dw_views.sql
│       └── init_dw.sql
│
├── .env
├── .env.etl
├── docker-compose.etl.yml
├── Manual Tecnico.txt
└── README.md

### Diagrama de arquitectura
flowchart LR
    A[(SQL Server OLTP)] -->|CDC Events| B[CDC Reader<br>replicator.py]

    B -->|Transform| C[ETL Processor<br>etl_run.py]
    C -->|Load| D[(PostgreSQL Data Warehouse)]

    D --> E[DW Views<br>KPIs / Materialized Views]

    E -->|Queries| F[Report-Service PHP]
    F -->|Cache| G[(Redis)]

    C -->|Invalidate keys| G

    H[Reconcilier<br>reconcile.py] --> D
    H --> A
Representa el flujo completo:
CDC → replicación → ETL → DW → KPIs → PHP → Redis

### Detalle de microservicios y lenguajes
| Servicio            | Lenguaje        | Rol                                        |
| ------------------- | --------------- | ------------------------------------------ |
| **etl**             | Python 3        | Procesos de transformación y carga         |
| **replicator**      | Python 3        | Lectura de CDC de SQL Server y replicación |
| **reconcile**       | Python 3        | Validaciones cruzadas OLTP ↔ DW            |
| **DW / PostgreSQL** | SQL / PL/pgSQL  | Data warehouse, staging, KPIs              |
| **Redis**           | Key-value store | Cache para reportería                      |

### Cómo levantar el sistema localmente 
# Crear red compartida
docker network create analytics-net || true
# Levantar el stack completo
docker compose --env-file .env.etl -f docker-compose.etl.yml up -d --build
# Verificar contenedores
docker ps
# Ver logs del replicador
docker logs -f etl-replicator

### Especificación de endpoints por microservicio
# Replicator (CDC Reader)
Ubicación: replication/replicator/
Funciones:
  ● Lee registros modificados desde SQL Server (CDC: Change Data Capture)
  ● Detecta inserts, updates, deletes
  ● Transforma el payload a formato estándar
  ● Envía registros a staging en PostgreSQL
  ● Archivos clave:
  ● cdc_reader.py
  ● replicator.py
  ● db.py
  ● transform_load.py
# ETL Processor
Ubicación: etl/etl_run.py
Funciones:
  ● Limpieza de datos
  ● Normalización
  ● Carga final a tablas del DW
  ● KPIs y materialized views
# Reconcile Service
Ubicación: /reconcile/reconcile.py
Funciones:
  ● Compara datos OLTP ↔ DW
  ● Detecta inconsistencias
  ● Genera alertas
  ● Registra métricas
# D. SQL Assets
Ubicación: /replication/sql/
Incluye:
  ● consultas de validación (checks_dw.sql)
  ● creación de DW (init_dw.sql)
  ● KPIs (dw_kpis.sql)
  ● vistas (dw_views.sql)
invalidación de redis:
  ● invalidate_on_etl.py
  ● invalidate_on_match_close.py

### Seguridad
# Redes
analytics-net: red Docker exclusiva del stack
# Acceso
JWT (roles):
  ● report_reader
  ● admin
# TLS
manejado desde Caddy (otra capa del servidor)
soporta:
  ● HSTS
  ● CSP
  ● Permissions Policy
# Logs
logging:
  options:
    max-size: "10m"
    max-file: "3"

### Bibliotecas
# Python
  ● psycopg2 → PostgreSQL
  ● pyodbc → SQL Server CDC
  ● redis → manejo de cache
  ● pydantic → validación de contratos
  ● yaml → lectura de catalog.yaml
  ● schedule → tareas programadas
  ● pandas → transformaciones rápidas (opcional)
# SQL Server
  ● CDC: Change Data Capture
  ● Scripts de auditoría
# PostgreSQL DW
  ● Vistas
  ● Materialized Views
  ● Staging layer
  ● Constraints

### Posibles errores y soluciones
# Posibles Errores y Soluciones
# Error: "CDC not enabled"
- Solución:
EXEC sys.sp_cdc_enable_db;
Ejecutar scripts:
replication/mssql/02A_enable_cdc.sql
# Error de conexión a PostgreSQL
Revisar en .env.etl:
POSTGRES_HOST=
POSTGRES_USER=
POSTGRES_PASSWORD=
# Redis no invalida cache
Revisar scripts:
  ● invalidate_on_etl.py
  ● invalidate_on_match_close.py
Además:
   docker logs redis
# Lag de replicación > 5 minutos
Ver logs del replicador:
docker logs -f etl-replicator
# Métricas esperadas:
etl_replication_lag_seconds < 60
# Tablas del DW no cargan
Revisar SQL:
init_dw.sql
dw_kpis.sql
dw_views.sql
- Ejecutar nuevamente:
docker exec -it etl-postgres psql -U postgres -f /scripts/init_dw.sql
