# Lógica asíncrona de consulta HTTP (asyncio + httpx)
# Descripcion: Lógica concurrente asíncrona de red. Implementa las tres corrutinas nominales
# de proveedores cloud (posts 1, 2 y 3 de JSONPlaceholder), los gatillos de caos real contra
# HttpBin (timeout y estatus HTTP erróneos) y el orquestador asyncio.TaskGroup que paraleliza
# todo, mapeando cada error nativo de httpx hacia una excepción semántica Tritón mediante
# encadenamiento explícito (raise ... from) y notas forenses con add_note().

# src/triton_telemetry/core.py
import asyncio
import json
import logging
from typing import Any, Awaitable, Callable, Dict, List, Union

import httpx

from .exceptions import (
    CorruptedPayloadError,
    NetworkPeeringError,
    ProviderTimeoutError,
    TritonError,
)

logger = logging.getLogger("triton_monitor")

# ---------------------------------------------------------------------------
# Consumo nominal real: un endpoint estable por proveedor (JSONPlaceholder)
# ---------------------------------------------------------------------------
PROVIDER_ENDPOINTS = {
    "AWS": "https://jsonplaceholder.typicode.com/posts/1",
    "Azure": "https://jsonplaceholder.typicode.com/posts/2",
    "GCP": "https://jsonplaceholder.typicode.com/posts/3",
}

# ---------------------------------------------------------------------------
# Endpoints de inyección de Caos real (HttpBin)
# ---------------------------------------------------------------------------
TIMEOUT_TRIGGER_URL = "https://httpbin.org/delay/3"             # Retardo controlado real de 3 segundos
GATEWAY_TIMEOUT_TRIGGER_URL = "https://httpbin.org/status/504"  # Estatus HTTP 504 (Gateway Timeout)
UNPROCESSABLE_TRIGGER_URL = "https://httpbin.org/status/422"    # Estatus HTTP 422 (Unprocessable Entity)

# Contexto forense estandarizado para auditoría (visible vía __notes__)
TIMEOUT_FORENSIC_NOTE = "Timeout superado en el nodo de telemetría de respaldo"
UNEXPECTED_STATUS_MESSAGE = "Estatus HTTP no esperado recibido"


async def _execute_telemetry_exchange(
    client: httpx.AsyncClient,
    provider: str,
    url: str,
    timeout: float,
) -> Dict[str, Any]:
    """
    Núcleo compartido de intercambio HTTP: ejecuta el GET asíncrono y traduce
    cada error nativo de httpx a su excepción semántica Tritón correspondiente.

    El encadenamiento explícito (raise ... from) preserva intacto el traceback
    de la causa raíz original para la auditoría forense posterior.
    """
    logger.debug(
        f"Petición asíncrona iniciada hacia {provider}: {url}",
        extra={"provider": provider},
    )

    try:
        response = await client.get(url, timeout=timeout)
        # Dispara httpx.HTTPStatusError si el servidor devuelve 4xx/5xx
        response.raise_for_status()
        payload = response.json()

    except httpx.TimeoutException as native_error:
        # Gatillo de timeout real: se re-lanza encadenado como ProviderTimeoutError
        semantic_error = ProviderTimeoutError(
            f"Se agotó el tiempo de espera ({timeout}s) consultando a {provider}."
        )
        semantic_error.add_note(TIMEOUT_FORENSIC_NOTE)
        semantic_error.add_note(f"Provider_ID: {provider} | Límite_CLI: {timeout}s | Endpoint: {url}")
        raise semantic_error from native_error

    except httpx.HTTPStatusError as native_error:
        # Estatus HTTP erróneo: se re-lanza encadenado como CorruptedPayloadError
        semantic_error = CorruptedPayloadError(UNEXPECTED_STATUS_MESSAGE)
        semantic_error.add_note(
            f"Provider_ID: {provider} | HTTP_Status_Code: {native_error.response.status_code}"
        )
        raise semantic_error from native_error

    except (json.JSONDecodeError, ValueError) as native_error:
        # Payload recibido pero no serializable a JSON estructurado
        raise CorruptedPayloadError(
            f"El proveedor {provider} devolvió un payload no serializable o con errores de paridad."
        ) from native_error

    except httpx.RequestError as native_error:
        # Caída física de transporte: DNS, ruteo, conexión rechazada, etc.
        semantic_error = NetworkPeeringError(
            f"Fallo físico de transporte o ruteo al intentar alcanzar {provider}."
        )
        semantic_error.add_note(f"Provider_ID: {provider} | Native_Error_Type: {type(native_error).__name__}")
        raise semantic_error from native_error

    if not isinstance(payload, dict):
        # Contrato de telemetría violado: el JSON es válido pero no es el objeto esperado
        raise CorruptedPayloadError(
            f"El proveedor {provider} devolvió un JSON válido pero fuera del contrato (tipo: {type(payload).__name__})."
        )

    logger.info(
        f"Telemetría recibida exitosamente de {provider}.",
        extra={"provider": provider, "status_code": response.status_code},
    )
    return {
        "provider": provider,
        "status": "NOMINAL",
        "latency_sec": response.elapsed.total_seconds(),
        "payload_id": payload.get("id", -1),
    }


# ---------------------------------------------------------------------------
# Requisito 1: Las tres corrutinas de red asíncronas (consumo nominal real)
# ---------------------------------------------------------------------------
async def query_aws_status(client: httpx.AsyncClient, timeout: float) -> Dict[str, Any]:
    """Corrutina nominal de AWS: consulta el post 1 de JSONPlaceholder."""
    return await _execute_telemetry_exchange(client, "AWS", PROVIDER_ENDPOINTS["AWS"], timeout)


async def query_azure_status(client: httpx.AsyncClient, timeout: float) -> Dict[str, Any]:
    """Corrutina nominal de Azure: consulta el post 2 de JSONPlaceholder."""
    return await _execute_telemetry_exchange(client, "Azure", PROVIDER_ENDPOINTS["Azure"], timeout)


async def query_gcp_status(client: httpx.AsyncClient, timeout: float) -> Dict[str, Any]:
    """Corrutina nominal de GCP: consulta el post 3 de JSONPlaceholder."""
    return await _execute_telemetry_exchange(client, "GCP", PROVIDER_ENDPOINTS["GCP"], timeout)


# ---------------------------------------------------------------------------
# Requisitos 2 y 3: Gatillos de inyección de fallos reales en producción
# ---------------------------------------------------------------------------
async def trigger_timeout_scenario(client: httpx.AsyncClient, timeout: float) -> Dict[str, Any]:
    """Gatillo de timeout real: /delay/3 tarda 3s en responder, de modo que un límite
    CLI inferior (ej. --timeout 1.0) dispara un httpx.TimeoutException genuino, que se
    captura y re-lanza encadenado como ProviderTimeoutError con nota forense dinámica."""
    return await _execute_telemetry_exchange(client, "AWS", TIMEOUT_TRIGGER_URL, timeout)


async def trigger_gateway_timeout_scenario(client: httpx.AsyncClient, timeout: float) -> Dict[str, Any]:
    """Gatillo de estatus erróneo 504: response.raise_for_status() eleva el
    httpx.HTTPStatusError nativo, re-lanzado como CorruptedPayloadError encadenado."""
    return await _execute_telemetry_exchange(client, "Azure", GATEWAY_TIMEOUT_TRIGGER_URL, timeout)


async def trigger_unprocessable_entity_scenario(client: httpx.AsyncClient, timeout: float) -> Dict[str, Any]:
    """Gatillo de estatus erróneo 422: valida la misma cadena de resiliencia ante
    Unprocessable Entity manteniendo íntegro el traceback de la causa raíz."""
    return await _execute_telemetry_exchange(client, "GCP", UNPROCESSABLE_TRIGGER_URL, timeout)


# ---------------------------------------------------------------------------
# Registro de misiones: selecciona la corrutina según modo operativo
# ---------------------------------------------------------------------------
TelemetryMission = Callable[[httpx.AsyncClient, float], Awaitable[Dict[str, Any]]]

NOMINAL_MISSIONS: Dict[str, TelemetryMission] = {
    "AWS": query_aws_status,
    "Azure": query_azure_status,
    "GCP": query_gcp_status,
}

CHAOS_MISSIONS: Dict[str, TelemetryMission] = {
    "AWS": trigger_timeout_scenario,
    "Azure": trigger_gateway_timeout_scenario,
    "GCP": trigger_unprocessable_entity_scenario,
}


# ---------------------------------------------------------------------------
# Requisito 4: Orquestación asíncrona mediante asyncio.TaskGroup
# ---------------------------------------------------------------------------
MissionOutcome = Union[Dict[str, Any], TritonError]


async def _run_mission_with_capture(
    mission: TelemetryMission,
    client: httpx.AsyncClient,
    provider: str,
    timeout: float,
) -> MissionOutcome:
    """
    Red de seguridad de continuidad operativa: aísla el fallo de cada proveedor
    para que una anomalía NO cancele a las tareas hermanas del TaskGroup
    (semántica fail-fast nativa). Devuelve el dict nominal o la excepción
    semántica ya encadenada como sentinela de incidente.
    """
    try:
        return await mission(client, timeout)
    except TritonError as semantic_error:
        logger.error(f"Incidente registrado en {provider}: {semantic_error}")
        return semantic_error


async def scan_all_providers(
    providers: list[str],
    timeout: float,
    use_chaos: bool = False,
) -> list[Dict[str, Any]]:
    """
    Coordina la ejecución paralela y simultánea de las corrutinas de telemetría
    dentro de un bloque async with asyncio.TaskGroup(), compartiendo un único
    httpx.AsyncClient entre todas las tareas.

    Tras completar TODAS las tareas (sin cancelaciones cruzadas), si existieron
    incidentes se re-elevan agrupados en un ExceptionGroup nativo, listo para la
    captura quirúrgica con except* en la capa de presentación (app_operator).
    """
    mission_registry = CHAOS_MISSIONS if use_chaos else NOMINAL_MISSIONS

    async with httpx.AsyncClient(follow_redirects=True) as client:
        async with asyncio.TaskGroup() as task_group:
            tasks = [
                task_group.create_task(
                    _run_mission_with_capture(mission_registry[provider], client, provider, timeout),
                    name=f"TritonTask-{provider}",
                )
                for provider in providers
            ]

    outcomes: List[MissionOutcome] = [task.result() for task in tasks]
    incidents = [outcome for outcome in outcomes if isinstance(outcome, TritonError)]

    if incidents:
        # Reconstrucción del ExceptionGroup con el inventario COMPLETO de fallos concurrentes
        raise ExceptionGroup("Incidentes de telemetría detectados por TaskGroup", incidents)

    return [outcome for outcome in outcomes if not isinstance(outcome, TritonError)]
