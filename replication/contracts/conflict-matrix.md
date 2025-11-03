# Matriz de conflictos (OLTP → DW)

| Caso | Descripción | Resolución |
|------|-------------|------------|
| U-1  | UPDATE en dimensión (p.ej., cambio de nombre de equipo) | **Type-1**: sobrescribir atributos en `dim_*` (mantener `vigente_desde` si cambia). |
| U-2  | UPDATE en jugador (cambio de equipo) | **Type-1**: actualizar `equipo_actual`. No reescribir hechos históricos. |
| U-3  | UPDATE en partido (cambio de fecha/hora) | Actualizar `dim_partido.fecha_hora`. Hechos referencian `sk_partido` sin cambio. |
| U-4  | UPDATE en anotación/falta (corrección de conteo) | `fact_*` por NK→SK: **upsert** (reinsertar la fila consolidada). |
| D-1  | DELETE en dimensión origen | **No borrar en DW** (mantener integridad). Marcar `activo=false` si aplica. |
| D-2  | DELETE en hechos origen | Opcional: no borrar; o marcar `cantidad=0`/`puntos=0` según política (evitar perder histórico). |
| C-1  | Arribo fuera de orden (hecho antes de dim) | **Reintento**: poner en **staging**; reintentar tras cargar dimensiones. |
| I-1  | Duplicados por reintento | **Idempotencia**: constraints UNIQUE en NK y `INSERT ... ON CONFLICT DO UPDATE`. |
| T-1  | Timestamps desalineados (timezone) | Usar **UTC** en todos los timestamps (`SYSUTCDATETIME` en OLTP; `TIMESTAMP WITH TIME ZONE` si decides). |
