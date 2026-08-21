# Lógica asíncrona de consulta HTTP (asyncio + httpx)
# Descripcion: Lógica concurrente asíncrona de red. Utiliza httpx.AsyncClient acoplado a un asyncio.TaskGroup para paralelizar peticiones reales a internet e intercepta errores lógicos/timeouts mapeándolos forensemente.

# src/triton_telemetry/core.py
import asyncio
import logging
import json
import httpx
from typing import Any, Dict
from .exceptions import ProviderTimeoutError, CorruptedPayloadError, NetworkPeeringError

logger = logging.getLogger("triton_monitor")

# Mapeo de proveedores reales de prueba (endpoints estables de JSONPlaceholder)
PROVIDER_ENDPOINTS = {
    "AWS": "https://jsonplaceholder.typicode.com/posts/1",
    "Azure": "https://jsonplaceholder.typicode.com/posts/2",
    "GCP": "https://jsonplaceholder.typicode.com/posts/3"
}

# Endpoints especiales para inyección de Caos real (vía HttpBin)
CHAOS_ENDPOINTS = {
    "TIMEOUT_TRIGGER": "https://httpbin.org/delay/3",    # Provoca retardo real de 3 segundos
    "BAD_GATEWAY_TRIGGER": "https://httpbin.org/status/504", # Provoca código real 504
    "CORRUPTED_TRIGGER": "https://httpbin.org/xml"       # Provoca datos XML cuando esperamos JSON
}


async def query_provider_telemetry(provider: str, timeout: float, use_chaos: bool = False) -> Dict[str, Any]:
    """
    Consulta la API de telemetría del proveedor de forma asíncrona usando httpx.
    Soporta inyección de caos realista para validar la resiliencia del sistema.
    """
    # Determinar el endpoint basado en el modo
    if use_chaos:
        if provider == "AWS":
            url = CHAOS_ENDPOINTS["TIMEOUT_TRIGGER"]
        elif provider == "Azure":
            url = CHAOS_ENDPOINTS["BAD_GATEWAY_TRIGGER"]
        else:
            url = CHAOS_ENDPOINTS["CORRUPTED_TRIGGER"]
    else:
        url = PROVIDER_ENDPOINTS.get(provider, "https://jsonplaceholder.typicode.com/posts/1")

    logger.debug(f"Petición asíncrona iniciada hacia {provider} en URL: {url}", extra={"provider": provider})

    # Usamos httpx.AsyncClient con control de timeouts estrictos
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, timeout=timeout)
            
            # Lanzará HTTPStatusError si el código es 4xx o 5xx
            response.raise_for_status()
            
            # Intento de parseo JSON estructurado
            try:
                data = response.json()
                logger.info(f"Telemetría recibida exitosamente de {provider}", extra={"provider": provider, "status_code": response.status_code})
                return {
                    "provider": provider,
                    "status": "NOMINAL",
                    "latency_sec": response.elapsed.total_seconds(),
                    "payload_id": data.get("id", -1)
                }
            except (json.JSONDecodeError, ValueError) as err:
                # Lanzar excepción semántica con encadenamiento
                raise CorruptedPayloadError(
                    f"El proveedor {provider} devolvió un payload no serializable o con errores de paridad."
                ) from err
                
        except httpx.TimeoutException as err:
            # Capturar timeouts nativos y relanzar semánticos agregándole notas de contexto
            p_err = ProviderTimeoutError(f"Se agotó el tiempo de espera ({timeout}s) al conectar con {provider}.")
            p_err.add_note(f"Provider_ID: {provider}")
            p_err.add_note(f"Requested_Timeout_Limit: {timeout}s")
            p_err.add_note(f"Target_Endpoint: {url}")
            raise p_err from err
            
        except httpx.HTTPStatusError as err:
            # Capturar respuestas erróneas de servidor y relanzar con metadatos
            n_err = NetworkPeeringError(f"Fallo de conexión o denegación de ruteo de {provider}. Estatus HTTP: {err.response.status_code}.")
            n_err.add_note(f"Provider_ID: {provider}")
            n_err.add_note(f"HTTP_Status_Code: {err.response.status_code}")
            raise n_err from err
            
        except httpx.RequestError as err:
            # Caída física o de red genérica (offline, dns fallido, etc.)
            n_err = NetworkPeeringError(f"Error crítico de transporte de red al intentar alcanzar {provider}.")
            n_err.add_note(f"Provider_ID: {provider}")
            n_err.add_note(f"Network_Error_Type: {type(err).__name__}")
            raise n_err from err


async def scan_all_providers(providers: list[str], timeout: float, use_chaos: bool = False) -> list[Dict[str, Any]]:
    """
    Orquesta las llamadas paralelas utilizando la estructura asyncio.TaskGroup.
    Todas las excepciones arrojadas por las tareas se agruparán en un ExceptionGroup nativo.
    """
    tasks = []
    results = []

    # El TaskGroup encapsula el ciclo de vida de la ejecución concurrente
    async with asyncio.TaskGroup() as tg:
        for provider in providers:
            # Asignamos nombres individuales a las tareas para facilitar la trazabilidad en logs
            task = tg.create_task(
                query_provider_telemetry(provider, timeout, use_chaos),
                name=f"Task-{provider}"
            )
            tasks.append(task)
            
    # Si la ejecución asíncrona es exitosa, colectamos los resultados de las tareas
    for task in tasks:
        results.append(task.result())
        
    return results