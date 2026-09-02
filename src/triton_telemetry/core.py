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
# FIX 1: faltaban Callable, Awaitable, List y Union — el módulo no cargaba
# (NameError) apenas Python intentaba interpretar las anotaciones de tipo
# más abajo (TelemetryMission, MissionOutcome, outcomes: List[...]).
from typing import Any, Awaitable, Callable, Dict, List, Union

import httpx

from .exceptions import (
    CorruptedPayloadError,
    NetworkPeeringError,
    ProviderTimeoutError,
    TritonError,           # se importa además de las subclases para poder
                            # capturar "cualquier incidente Tritón" de forma
                            # genérica en _run_mission_with_capture()
)

logger = logging.getLogger("triton_monitor")  # logger central de la app_operator


# Consumo nominal real: un endpoint estable por proveedor (JSONPlaceholder)
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
CORRUPTED_TRIGGER_URL = "https://httpbin.org/xml"               # HTTP 200 + cuerpo XML → el parseo JSON falla →
                                                                 # CorruptedPayloadError (Escenario C de la consigna)

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
        # await client.get(...) libera el event loop mientras espera la
        # respuesta: otras tareas del TaskGroup pueden avanzar mientras tanto.
        response = await client.get(url, timeout=timeout)

        # raise_for_status() convierte cualquier 4xx/5xx en httpx.HTTPStatusError,
        # así lo podemos capturar y traducir más abajo en vez de seguir de largo
        # con una respuesta inválida.
        response.raise_for_status()
        payload = response.json()

    except httpx.TimeoutException as native_error:
        # Se disparó porque el servidor tardó más que `timeout` (caso real:
        # TIMEOUT_TRIGGER_URL tarda 3s fijos). Se traduce a una excepción
        # propia del dominio Tritón en vez de dejar filtrar el tipo de httpx.
        semantic_error = ProviderTimeoutError(
            f"Se agotó el tiempo de espera ({timeout}s) consultando a {provider}."
        )
        # add_note() adjunta contexto extra sin pisar el mensaje original,
        # útil para logs/auditoría posterior sin perder la excepción base.
        semantic_error.add_note(TIMEOUT_FORENSIC_NOTE)
        semantic_error.add_note(f"Provider_ID: {provider} | Límite_CLI: {timeout}s | Endpoint: {url}")
        # "raise ... from native_error" encadena la excepción: el traceback
        # muestra la causa real de httpx, no solo la semántica de Tritón.
        raise semantic_error from native_error

    except httpx.HTTPStatusError as native_error:
        # Criterio de los textos de rol de la consigna (§2.2.1/§2.2.2): los
        # estatus HTTP fallidos (4xx/5xx) son "respuestas corruptas o estatus
        # fallidos HTTP" y se traducen a CorruptedPayloadError, exactamente
        # como prescribe Integrante 2: "raise CorruptedPayloadError(...)
        # from error_nativo". NetworkPeeringError queda exclusivamente para
        # fallos de DNS/ruteo/resolución de hosts (httpx.RequestError, abajo).
        semantic_error = CorruptedPayloadError(
            f"El proveedor {provider} respondió con un error HTTP: "
            f"{native_error.response.status_code}."
        )
        semantic_error.add_note(
            f"Provider_ID: {provider} | HTTP_Status_Code: {native_error.response.status_code}"
        )
        raise semantic_error from native_error

    except (json.JSONDecodeError, ValueError) as native_error:
        # JSONDecodeError hereda de ValueError, por eso alcanza con capturar
        # ambos en un solo except: cubre "no vino JSON" y "vino JSON mal
        # formado" con el mismo tratamiento.
        semantic_error = CorruptedPayloadError(
            f"El proveedor {provider} devolvió un payload no serializable o con errores de paridad."
        )
        # `response` está garantizado en scope en este except: solo puede
        # dispararse desde response.json(), o sea DESPUÉS de que el GET y
        # raise_for_status() completaron (caso real del Escenario C: GCP
        # devuelve XML con HTTP 200 y el parseo JSON explota acá). Sin estas
        # notas el incidente llegaría a consola sin salida FORENSE.
        semantic_error.add_note(
            f"Provider_ID: {provider} | Endpoint: {url} | "
            f"Content-Type: {response.headers.get('content-type', 'desconocido')}"
        )
        raise semantic_error from native_error

    except httpx.RequestError as native_error:
        # Cubre fallas de transporte antes de siquiera recibir una respuesta:
        # DNS caído, conexión rechazada, problemas de ruteo, etc.
        semantic_error = NetworkPeeringError(
            f"Fallo físico de transporte o ruteo al intentar alcanzar {provider}."
        )
        semantic_error.add_note(f"Provider_ID: {provider} | Native_Error_Type: {type(native_error).__name__}")
        raise semantic_error from native_error

    if not isinstance(payload, dict):
        # JSON válido no implica estructura válida: por ejemplo [1, 2, 3]
        # parsea bien pero no tiene .get(), y rompería la línea de abajo.
        contract_error = CorruptedPayloadError(
            f"El proveedor {provider} devolvió un JSON válido pero fuera del contrato (tipo: {type(payload).__name__})."
        )
        contract_error.add_note(
            f"Provider_ID: {provider} | Contrato: objeto JSON esperado, "
            f"recibido {type(payload).__name__} | Endpoint: {url}"
        )
        raise contract_error

    logger.info(
        f"Telemetría recibida exitosamente de {provider}.",
        extra={"provider": provider, "status_code": response.status_code},
    )
    return {
        "provider": provider,
        "status": "NOMINAL",
        # response.elapsed ya trae la duración medida por httpx: más preciso
        # y prolijo que tomar el tiempo manualmente con el loop.
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
    httpx.HTTPStatusError nativo, re-lanzado como CorruptedPayloadError encadenado
    con su HTTP_Status_Code en las notas forenses (estatus HTTP fallido, según
    el texto de roles §2.2.1/§2.2.2 de la consigna)."""
    return await _execute_telemetry_exchange(client, "Azure", GATEWAY_TIMEOUT_TRIGGER_URL, timeout)


async def trigger_corrupted_payload_scenario(client: httpx.AsyncClient, timeout: float) -> Dict[str, Any]:
    """Gatillo de payload corrupto: /xml responde HTTP 200 con un cuerpo XML, de
    modo que response.json() dispara un json.JSONDecodeError genuino, capturado y
    re-lanzado encadenado como CorruptedPayloadError con notas forenses de
    Provider_ID/Endpoint/Content-Type (Escenario C de la consigna: GCP falla por
    formato de payload corrupto al devolver XML)."""
    return await _execute_telemetry_exchange(client, "GCP", CORRUPTED_TRIGGER_URL, timeout)


# ---------------------------------------------------------------------------
# Registro de misiones: selecciona la corrutina según modo operativo
# ---------------------------------------------------------------------------
# Alias de tipo: una "misión" es cualquier corrutina que toma (client, timeout)
# y devuelve eventualmente un dict de telemetría. Sirve para tipar los
# diccionarios de abajo sin repetir la firma completa cada vez.
TelemetryMission = Callable[[httpx.AsyncClient, float], Awaitable[Dict[str, Any]]]

# Registro nominal: qué corrutina "real" corresponde a cada proveedor.
NOMINAL_MISSIONS: Dict[str, TelemetryMission] = {
    "AWS": query_aws_status,
    "Azure": query_azure_status,
    "GCP": query_gcp_status,
}

# Registro de caos: mismo shape que el de arriba pero apuntando a los
# gatillos de fallo. Permite elegir el diccionario completo con un solo
# booleano (use_chaos) en vez de un if/else por proveedor.
CHAOS_MISSIONS: Dict[str, TelemetryMission] = {
    "AWS": trigger_timeout_scenario,
    "Azure": trigger_gateway_timeout_scenario,
    "GCP": trigger_corrupted_payload_scenario,
}


# ---------------------------------------------------------------------------
# Requisito 4: Orquestación asíncrona mediante asyncio.TaskGroup
# ---------------------------------------------------------------------------
# El resultado de una misión puede ser el dict nominal o, si falló, la propia
# excepción semántica ya capturada (en vez de dejarla explotar sin control).
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
        # Clave del wrapper: en vez de dejar que la excepción se propague
        # dentro del TaskGroup (lo que cancelaría las demás tareas por el
        # comportamiento fail-fast nativo), la atrapamos acá y la devolvemos
        # como un valor más. Así todas las tareas llegan a completarse.
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
    # Selecciona de una sola vez el diccionario de corrutinas a usar según el
    # modo (nominal vs. caos), evitando ramificar la lógica en cada proveedor.
    mission_registry = CHAOS_MISSIONS if use_chaos else NOMINAL_MISSIONS

    # Un solo AsyncClient compartido por todas las tareas: reutiliza el pool
    # de conexiones en vez de abrir/cerrar una conexión por request.
    async with httpx.AsyncClient(follow_redirects=True) as client:
        async with asyncio.TaskGroup() as task_group:
            tasks = [
                task_group.create_task(
                    # Se llama siempre a través del wrapper _run_mission_with_capture,
                    # nunca a la misión "pelada" — así ningún proveedor caído
                    # tumba a los demás.
                    _run_mission_with_capture(mission_registry[provider], client, provider, timeout),
                    name=f"TritonTask-{provider}",
                )
                for provider in providers
            ]

    # Acá ya salimos del "async with TaskGroup()", así que todas las tareas
    # terminaron (con éxito o con incidente capturado) — es seguro leer .result().
    outcomes: List[MissionOutcome] = [task.result() for task in tasks]
    incidents = [outcome for outcome in outcomes if isinstance(outcome, TritonError)]

    if incidents:
        # Empaqueta TODOS los incidentes juntos (no solo el primero) en un
        # ExceptionGroup nativo de Python 3.11+, pensado para capturarse
        # selectivamente más arriba con "except*".
        raise ExceptionGroup("Incidentes de telemetría detectados por TaskGroup", incidents)

    return [outcome for outcome in outcomes if not isinstance(outcome, TritonError)]
