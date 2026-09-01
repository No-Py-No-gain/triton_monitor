# CHANGELOG — PROYECTO TRITÓN

Registro técnico de cambios compartido con el equipo de desarrollo.

---

## [1.0.0] — 2026-08-23

**Rama:** `desarrollo/optimizacion-triton`
**Base:** `Core_Secu`
**Alcance:** Integrante 2 — Ingeniería de Concurrencia y Telemetría Asíncrona (`core.py`) + correcciones de integración detectadas en pruebas reales de red.

### Added (Agregado)

#### Motor asíncrono de telemetría (`src/triton_telemetry/core.py`)

- **Corrutinas nominales de proveedores cloud** con consumo real vía `httpx.AsyncClient`:
  - `query_aws_status()` → JSONPlaceholder post 1.
  - `query_azure_status()` → JSONPlaceholder post 2.
  - `query_gcp_status()` → JSONPlaceholder post 3.
- **Núcleo de intercambio HTTP compartido** `_execute_telemetry_exchange()`:
  - Traducción de cada error nativo de `httpx` a una excepción semántica Tritón mediante **encadenamiento explícito** (`raise ... from native_error`), preservando íntegro el traceback de la causa raíz:
    - `httpx.TimeoutException` → `ProviderTimeoutError`.
    - `httpx.HTTPStatusError` (vía `response.raise_for_status()`) → `CorruptedPayloadError`.
    - `json.JSONDecodeError / ValueError` → `CorruptedPayloadError`.
    - `httpx.RequestError` (DNS, ruteo, conexión rechazada) → `NetworkPeeringError`.
  - Inyección de **contexto forense dinámico** con `add_note()`: nota estandarizada `"Timeout superado en el nodo de telemetría de respaldo"` más metadatos por incidente (`Provider_ID`, `Límite_CLI`, `Endpoint`, `HTTP_Status_Code`).
  - Validación adicional de contrato: si la respuesta es JSON válido pero no es un objeto (`list`, `str`, etc.), se lanza `CorruptedPayloadError`.
- **Gatillos de inyección de caos real** contra HttpBin para validar resiliencia en producción:
  - `trigger_timeout_scenario()` → `https://httpbin.org/delay/3`: con `--timeout 1.0` dispara un `httpx.ReadTimeout` genuino.
  - `trigger_gateway_timeout_scenario()` → `https://httpbin.org/status/504`.
  - `trigger_unprocessable_entity_scenario()` → `https://httpbin.org/status/422`.
- **Registros de misiones desacoplados** (`NOMINAL_MISSIONS` / `CHAOS_MISSIONS`): mapeo declarativo `provider → corrutina` seleccionable por bandera CLI `--chaos`.

#### Orquestación concurrente

- `scan_all_providers()`: paraleliza las tres consultas dentro de un bloque `async with asyncio.TaskGroup()`, compartiendo una única instancia de `httpx.AsyncClient` entre todas las tareas (reutilización de conexión/pool).
- **Red de seguridad anti-fail-fast** `_run_mission_with_capture()` (ver *Fixed*): aísla el fallo de cada proveedor y devuelve el resultado nominal o la excepción semántica como sentinela; tras completar TODAS las tareas, los incidentes se re-elevan agrupados en un `ExceptionGroup` nativo listo para captura quirúrgica con `except*` en la capa CLI.

#### Frontera CLI (`src/app_operator.py`)

- Blindaje de consola Windows: `sys.stdout.reconfigure(encoding="utf-8")` cuando el código activo no es UTF-8.
- Activación del árbol forense JSON: `exc_info=exc` en los 4 manejadores `except*`.

### Fixed (Corregido)

1. **Cancelación cruzada del TaskGroup (semántica fail-fast nativa)** — *crítico*:
   - **Síntoma:** al probar caos con `--timeout 1.0`, solo llegaba 1 incidente al `except*`. Cuando GCP devolvió su HTTP 422 primero, el TaskGroup canceló a las tareas hermanas: AWS fue abortado antes de que su timeout real se materializara y el 504 de Azure fue cancelado en vuelo.
   - **Impacto:** un monitor de telemetría debe reportar el panorama completo de los tres proveedores en cada ciclo, no abortar ante el primer fallo.
   - **Solución:** aislamiento por tarea + reconstrucción del `ExceptionGroup` con el inventario completo de fallos tras finalizar todas las corrutinas.
2. **UnicodeEncodeError en consola Windows**: el codec cp1252 no puede codificar glifos del pipeline forense (`└─`, `Ó`), corrompiendo la salida ERROR del logger. Resuelto reconfigurando stdout a UTF-8 en la frontera CLI.
3. **Árbol de excepciones dormido en el log JSON**: el serializador recursivo de `AsyncJSONFormatter` (que expande ExceptionGroups, notas `__notes__` y causas `__cause__`) nunca se activaba porque nadie registraba incidentes con `exc_info`. Ahora cada incidente audita su cadena completa en `triton_services.log`.

### Changed (Modificado)

- `requirements.txt`: dependencia efectiva `httpx>=0.27` declarada (antes solo existía el comentario).

### Validación (evidencia de ejecución real)

| Escenario | Comando | Resultado |
|---|---|---|
| Escaneo nominal | `python app_operator.py AWS Azure GCP -c cluster-us-east-01 -t 2.5` | 3 proveedores NOMINAL en ~1 s wall-time total (paralelismo demostrado; secuencial ≈ 2.5 s). Exit code 0. |
| Caos: timeout real | `... -t 1.0 --chaos` | `httpcore.ReadTimeout → httpx.ReadTimeout → ProviderTimeoutError` encadenados, con notas forenses visibles. |
| Caos: estatus erróneos | `... -t 1.0 --chaos` | Azure 504 y GCP 422 capturados como `CorruptedPayloadError` con `HTTP_Status_Code` en notas. |
| Sanitizador timeout | `-t 7` | `argparse.ArgumentTypeError`, salida limpia con **código 2**. |
| Sanitizador clúster | `-c cluster-MADRID-99` | Regex rechaza formato inválido, **código 2**. |

Ejemplo de cadena auditada en el log JSON:

```
httpcore.ReadTimeout
  └─ causa directa → httpx.ReadTimeout
       └─ causa directa (raise ... from) → triton_telemetry.exceptions.ProviderTimeoutError
            Notas: "Timeout superado en el nodo de telemetría de respaldo"
                   "Provider_ID: AWS | Límite_CLI: 1.0s | Endpoint: https://httpbin.org/delay/3"
```

### Notas de integración para el equipo

- Los cambios 2 y 3 de la sección *Fixed* modifican `src/app_operator.py` (frontera CLI). Se solicita al responsable de ese módulo validarlos en su flujo.
- Contrato consumido del Integrante 1 (sin modificar sus archivos):
  - `exceptions.py`: `TritonError`, `ProviderTimeoutError`, `CorruptedPayloadError`, `NetworkPeeringError`.
  - `sanitizer.py`: `parse_timeout` (rango estricto [0.1, 5.0]) garantiza que solo timeouts válidos alcancen el bucle de eventos; `parse_cluster_id` valida el patrón `cluster-<region>-<numero>` previo a cualquier I/O de red.
- Requisitos de entorno: Python ≥ 3.11 (`asyncio.TaskGroup`, `except*`, `add_note`), httpx ≥ 0.27.
