# =============================================================================
# módulo: core.py — Núcleo asíncrono de telemetría (asyncio + httpx)
# =============================================================================
# ROL EN LA ARQUITECTURA:
#   Este módulo es la "capa de transporte y orquestación" del paquete
#   triton_telemetry. No contiene la UI ni el parseo de argumentos de la CLI
#   (eso vive en app_operator.py), sino SOLO la lógica de red:
#
#     1) tres corrutinas NOMINALES que consultan un endpoint real y estable
#        por proveedor cloud (AWS, Azure, GCP) usando JSONPlaceholder;
#     2) tres gatillos de CAOS que consultan endpoint reales de HttpBin que
#        inyectan fallos deliberados (timeout, 504, 422) para probar que el
#        sistema de resiliencia reacciona correctamente;
#     3) un orquestador asyncio.TaskGroup que lanza todas las consultas de
#        forma CONCURRENTE (en paralelo dentro de un mismo event loop);
#     4) un traductor sistemático que mapea cada error nativo de httpx hacia
#        una excepción SEMÁNTICA del dominio Tritón (exceptions.py), preservando
#        la causa raíz con "raise ... from" y adjuntando contexto con add_note().
#
# RELACIONES DEL PAQUETE:
#   - exceptions.py  -> define TritonError y sus subclases (las que usamos aquí).
#   - sanitizer.py   -> parsea y valida los argumentos CLI (timeout, cluster_id).
#   - logging_engine.py -> configura el logging (QueueHandler + QueueListener).
#   - __init__.py    -> exporta la API pública; importa scan_all_providers de aquí.
#   - app_operator.py-> consumidor CLI: llama a scan_all_providers() y captura
#                      el ExceptionGroup con "except*".
# =============================================================================
# Descripcion: Lógica concurrente asíncrona de red. Implementa las tres corrutinas nominales
# de proveedores cloud (posts 1, 2 y 3 de JSONPlaceholder), los gatillos de caos real contra
# HttpBin (timeout y estatus HTTP erróneos) y el orquestador asyncio.TaskGroup que paraleliza
# todo, mapeando cada error nativo de httpx hacia una excepción semántica Tritón mediante
# encadenamiento explícito (raise ... from) y notas forenses con add_note().

# src/triton_telemetry/core.py

# ---------------------------------------------------------------------------
# IMPORTS: qué traemos y por qué
# ---------------------------------------------------------------------------
import asyncio          # Proporciona el event loop, TaskGroup (concurrencia) y
                        # asyncio.run() para arrancar la máquina asíncrona.
import json             # Para interpretar el cuerpo de la respuesta HTTP (JSON).
                        # Necesario también para capturar json.JSONDecodeError.
import logging          # Logger jerárquico "triton_monitor"; cada módulo registra
                        # eventos en ese mismo logger centralizado.

# FIX 1: faltaban Callable, Awaitable, List y Union — el módulo no cargaba
# (NameError) apenas Python intentaba interpretar las anotaciones de tipo
# más abajo (TelemetryMission, MissionOutcome, outcomes: List[...]).
from typing import Any, Awaitable, Callable, Dict, List, Union
#   - Any        : valor libre (los dicts de resultado pueden tener cualquier shape).
#   - Awaitable  : para tipar que una "misión" es una corrutina (objeto awaitable).
#   - Callable   : para el alias TelemetryMission (función que toma client+timeout).
#   - Dict/List  : contenedores tipados (Python usa dict/list en runtime; estas
#                  anotaciones ayudan a los linters y al IDE, no al runtime).
#   - Union      : para MissionOutcome = Dict OR TritonError (unión de tipos).

import httpx           # Cliente HTTP asíncrono: async with httpx.AsyncClient, el
                       # método .get(..., timeout=) que respeta el event loop y sus
                       # clases de excepción (TimeoutException, HTTPStatusError...).

# try: importa desde el PAQUETE (uso normal, vía app_operator -> triton_telemetry)
# except ImportError: fallback a import ABSOLUTO del módulo local del mismo
# directorio (solo para poder correr "python core.py" como script de prueba).
try:
    from .exceptions import (          # el "." = "en la carpeta actual del paquete"
        CorruptedPayloadError,      # respuesta JSON corrupta o fuera de contrato
        NetworkPeeringError,        # fallo de transporte/ruteo/DNS o 4xx/5xx
        ProviderTimeoutError,       # el proveedor excedió el límite de tiempo
        TritonError,       # base de todas las excepciones Tritón; se importa para
                            # capturar genéricamente "cualquier incidente Tritón"
                            # en _run_mission_with_capture()
    )
except ImportError:
    # Solo se dispara cuando ejecutamos "python core.py" directamente (fuera de
    # un paquete). Python no conoce el paquete triton_telemetry, así que el "." no
    # resuelve; recurrimos al archivo exceptions.py que está en el mismo directorio.
    from exceptions import (
        CorruptedPayloadError,
        NetworkPeeringError,
        ProviderTimeoutError,
        TritonError,
    )

logger = logging.getLogger("triton_monitor")  # logger central de la app_operator


# ---------------------------------------------------------------------------
# ENDPOINTS DE CONSUMO NOMINAL (modo real, sin fallos inyectados)
# ---------------------------------------------------------------------------
# JSONPlaceholder (jsonplaceholder.typicode.com) es una API pública gratuita y
# estable, ideal para prácticas. "%s/1/2/3" devuelve objetos JSON con la forma:
#   {"userId": 1, "id": 1, "title": "...", ...}
# Así cada proveedor consulta un recurso DIFERENTE (post 1, 2 o 3) y podemos
# distinguirlos por el campo "id" / "payload_id" sin necesidad de base de datos.
PROVIDER_ENDPOINTS = {
    "AWS": "https://jsonplaceholder.typicode.com/posts/1",
    "Azure": "https://jsonplaceholder.typicode.com/posts/2",
    "GCP": "https://jsonplaceholder.typicode.com/posts/3",
}

# ---------------------------------------------------------------------------
# Endpoints de inyección de Caos real (HttpBin)
# ---------------------------------------------------------------------------
# HttpBin (httpbin.org) devuelve respuestas HTTP "custom" a pedido. Estos tres
# endpoints generan fallos REALES y predecibles que gatillan cada rama del
# traductor de errores:
TIMEOUT_TRIGGER_URL = "https://httpbin.org/delay/3"
#   - /delay/3: el servidor tarda 3 segundos ENTEROS en responder. Si nuestro
#     timeout CLI es menor (ej. -t 1.0), httpx lanza httpx.ReadTimeout y aquí se
#     traduce a ProviderTimeoutError.
GATEWAY_TIMEOUT_TRIGGER_URL = "https://httpbin.org/status/504"
#   - /status/504: devuelve estatus HTTP "504 Gateway Timeout". raise_for_status()
#     lo convierte en httpx.HTTPStatusError -> se traduce a NetworkPeeringError.
UNPROCESSABLE_TRIGGER_URL = "https://httpbin.org/status/422"
#   - /status/422: devuelve estatus HTTP "422 Unprocessable Entity" (error del
#     cliente). Mismo tratamiento que el 504 -> NetworkPeeringError.

# Contexto forense estandarizado para auditoría (visible vía __notes__)
# Estas constantes se pasan a .add_note() para que cada excepción lleve contexto
# extra legible en los logs, sin pisar el mensaje original.
TIMEOUT_FORENSIC_NOTE = "Timeout superado en el nodo de telemetría de respaldo"
UNEXPECTED_STATUS_MESSAGE = "Estatus HTTP no esperado recibido"


# ---------------------------------------------------------------------------
# NÚCLEO COMPARTIDO DE INTERCAMBIO HTTP (el "cerebro" de la traducción)
# ---------------------------------------------------------------------------
# Esta función es el punto único por el que pasa TODA consulta HTTP, sea nominal
# o de caos. Centralizar aquí la lógica evita duplicar el try/except en cada
# corrutina y garantiza que la traducción de errores sea idéntica en todos los
# proveedores. Por eso se suele llamar "helper"/"núcleo compartido".
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
    # El 'extra={...}' enriquece el registro con datos estructurados que el
    # formateador de logging_engine puede mostrar o filtrar. Aquí solo aporta
    # contexto de qué proveedor inició la petición.
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
        # response.json() interpreta el cuerpo como JSON; si el servidor no
        # devolvió JSON válido lanza json.JSONDecodeError (capturado más abajo).
        payload = response.json()

    # ---- TRADUCCIÓN DE ERRORES: cada "except" captura un error nativo de
    #      httpx y lo convierte en la excepción semántica del dominio Tritón.

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
        # FIX 2: un 504/422 es un problema de RED/SERVIDOR, no de payload
        # corrupto — antes esto mapeaba (mal) a CorruptedPayloadError y
        # mezclaba dos causas distintas bajo la misma excepción semántica.
        # Ahora coincide con el criterio usado en el resto del proyecto:
        # fallas de transporte/estatus HTTP -> NetworkPeeringError.
        semantic_error = NetworkPeeringError(
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
        raise CorruptedPayloadError(
            f"El proveedor {provider} devolvió un payload no serializable o con errores de paridad."
        ) from native_error

    except httpx.RequestError as native_error:
        # Cubre fallas de transporte antes de siquiera recibir una respuesta:
        # DNS caído, conexión rechazada, problemas de ruteo, etc.
        semantic_error = NetworkPeeringError(
            f"Fallo físico de transporte o ruteo al intentar alcanzar {provider}."
        )
        semantic_error.add_note(f"Provider_ID: {provider} | Native_Error_Type: {type(native_error).__name__}")
        raise semantic_error from native_error

    # IMPORTANTE: el try/except NO captura este chequeo de abajo, porque aquí
    # ya NO hay error de red — el problema es de ESTRUCTURA de datos.
    if not isinstance(payload, dict):
        # JSON válido no implica estructura válida: por ejemplo [1, 2, 3]
        # parsea bien pero no tiene .get(), y rompería la línea de abajo.
        raise CorruptedPayloadError(
            f"El proveedor {provider} devolvió un JSON válido pero fuera del contrato (tipo: {type(payload).__name__})."
        )

    logger.info(
        f"Telemetría recibida exitosamente de {provider}.",
        extra={"provider": provider, "status_code": response.status_code},
    )
    # Este es el "contrato de salida" (shape) que consumen las corrutinas y que
    # luego aparecerá en la consola de app_operator: proveedor, estado, latencia
    # medida por la propia librería (response.elapsed) y el id del payload.
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
# Las funciones definidas con "async def" son CORRUTINAS: cuando se llaman NO
# ejecutan su cuerpo de inmediato, devuelven un objeto "awaitable" que debe ser
# consumido por el event loop. Aquí cada una es una fina envoltura que delega en
# el núcleo compartido _execute_telemetry_exchange pasándole su proveedor y URL.
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
# Contraparte de caos de las anteriores. Mismo "shape" (misma firma) pero con
# las URLs de HttpBin que inyectan fallos REALES. Al mantener la misma firma, el
# código de orquestación puede usar el registro nominal O el de caos sin cambiar
# nada de su lógica (ver CHAOS_MISSIONS/NOMINAL_MISSIONS y `use_chaos`).
async def trigger_timeout_scenario(client: httpx.AsyncClient, timeout: float) -> Dict[str, Any]:
    """Gatillo de timeout real: /delay/3 tarda 3s en responder, de modo que un límite
    CLI inferior (ej. --timeout 1.0) dispara un httpx.TimeoutException genuino, que se
    captura y re-lanza encadenado como ProviderTimeoutError con nota forense dinámica."""
    return await _execute_telemetry_exchange(client, "AWS", TIMEOUT_TRIGGER_URL, timeout)


async def trigger_gateway_timeout_scenario(client: httpx.AsyncClient, timeout: float) -> Dict[str, Any]:
    """Gatillo de estatus erróneo 504: response.raise_for_status() eleva el
    httpx.HTTPStatusError nativo, re-lanzado como NetworkPeeringError encadenado."""
    return await _execute_telemetry_exchange(client, "Azure", GATEWAY_TIMEOUT_TRIGGER_URL, timeout)


async def trigger_unprocessable_entity_scenario(client: httpx.AsyncClient, timeout: float) -> Dict[str, Any]:
    """Gatillo de estatus erróneo 422: valida la misma cadena de resiliencia ante
    Unprocessable Entity manteniendo íntegro el traceback de la causa raíz."""
    return await _execute_telemetry_exchange(client, "GCP", UNPROCESSABLE_TRIGGER_URL, timeout)


# ---------------------------------------------------------------------------
# Registro de misiones: selecciona la corrutina según modo operativo
# ---------------------------------------------------------------------------
# "Registro" es una tabla/mapa (dict) que asocia una CLAVE (el nombre del
# proveedor, ej. "AWS") con una FUNCIÓN (la corrutina a ejecutar). Esto evita
# escribir cadenas de if/elif por proveedor y hace trivial cambiar el
# comportamiento con una bandera booleana.
#
# Alias de tipo: una "misión" es cualquier corrutina que toma (client, timeout)
# y devuelve eventualmente un dict de telemetría. Sirve para tipar los
# diccionarios de abajo sin repetir la firma completa cada vez.
#   Callable[[httpx.AsyncClient, float], Awaitable[Dict[str, Any]]] se lee:
#   "función que recibe (AsyncClient, float) y devuelve un Awaitable que, al
#    terminar, produce un Dict[str, Any]".
TelemetryMission = Callable[[httpx.AsyncClient, float], Awaitable[Dict[str, Any]]]

# Registro nominal: qué corrutina "real" corresponde a cada proveedor.
# Es el modo normal de operación (consulta JSONPlaceholder).
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
    "GCP": trigger_unprocessable_entity_scenario,
}


# ---------------------------------------------------------------------------
# Requisito 4: Orquestación asíncrona mediante asyncio.TaskGroup
# ---------------------------------------------------------------------------
# El resultado de una misión puede ser el dict nominal o, si falló, la propia
# excepción semántica ya capturada (en vez de dejarla explotar sin control).
# MissionOutcome = "o un dict de telemetría, o una excepción Tritón".
MissionOutcome = Union[Dict[str, Any], TritonError]


# ---------------------------------------------------------------------------
# WRAPPER DE CAPTURA (patrón "sentinela")
# ---------------------------------------------------------------------------
# Este wrapper envuelve CADA misión. Su objetivo es impedir que la excepción de
# un proveedor se PROPAGUE fuera de su propia tarea dentro del TaskGroup, porque
# el TaskGroup tiene semántica "fail-fast": si una tarea lanza una excepción no
# capturada, CANCELA todas las tareas hermanas que aún no terminaron. Para
# evitarlo, capturamos la excepción Tritón y la DEVOLVEMOS como un valor normal;
# de esa forma todas las tareas llegan a completarse y nadie es cancelado.
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
        # Si la misión termina bien, devolvemos el dict de telemetría normal.
        return await mission(client, timeout)
    except TritonError as semantic_error:
        # Clave del wrapper: en vez de dejar que la excepción se propague
        # dentro del TaskGroup (lo que cancelaría las demás tareas por el
        # comportamiento fail-fast nativo), la atrapamos acá y la devolvemos
        # como un valor más. Así todas las tareas llegan a completarse.
        # Capturamos TritonError (la base) porque TODAS las excepciones
        # semánticas heredan de ella; el fallo ya fue traducido y encadenado
        # dentro de _execute_telemetry_exchange.
        logger.error(f"Incidente registrado en {provider}: {semantic_error}")
        # Devolver la excepción (en vez de lanzarla) es el "sentinela de
        # incidente": el orquestador la detectará abajo con isinstance().
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
    # ---------------------------------------------------------------------------
    # 1) Elegir el "mapa" de misiones según la bandera use_chaos.
    #    Elegimos el diccionario COMPLETO de una vez: si es caos se usan los
    #    gatillos de fallo (CHAOS_MISSIONS); si no, las consultas normales
    #    (NOMINAL_MISSIONS). Así NO necesitamos un if/else dentro del bucle.
    # ---------------------------------------------------------------------------
    mission_registry = CHAOS_MISSIONS if use_chaos else NOMINAL_MISSIONS

    # ---------------------------------------------------------------------------
    # 2) "async with httpx.AsyncClient(...)": crea un ÚNICO cliente HTTP y
    #    garantiza su cierre automático al salir del bloque. Reutilizar un solo
    #    cliente para todas las tareas aprovecha el POOL DE CONEXIONES (keep-
    #    alive), en vez de abrir/cerrar una conexión TCP por cada request.
    #    follow_redirects=True permite que HttpBin/JSONPlaceholder nos sigan
    #    re-dirigiendo si responden con un 3xx.
    # ---------------------------------------------------------------------------
    async with httpx.AsyncClient(follow_redirects=True) as client:
        # -----------------------------------------------------------------------
        # 3) asyncio.TaskGroup: el orquestador de concurrencia (Python 3.11+).
        #    Lanza todas las corrutinas como TAREAS que compiten por el MISMO
        #    event loop y avanzan de forma INTERCALADA cuando hay un "await"
        #    (cada petición HTTP libera el loop mientras espera la respuesta).
        #    El "async with" además espera a que TODAS las tareas terminen antes
        #    de continuar, y agrupa cualquier excepción no capturada.
        # -----------------------------------------------------------------------
        async with asyncio.TaskGroup() as task_group:
            # Lista por comprensión: por cada proveedor creamos UNA tarea.
            #   - create_task(...): registra la corrutina para que el loop la
            #     ejecute concurrentemente y devuelve un objeto Task.
            #   - LLAMAMOS SIEMPRE al wrapper _run_mission_with_capture (nunca
            #     a la misión "pelada") para que un proveedor que falle NO
            #     cancele a los demás (fail-fast). El wrapper captura el error y
            #     lo devuelve como valor, no como excepción propagada.
            #   - name="TritonTask-<provider>": etiqueta útil para debug/repr.
            tasks = [
                task_group.create_task(
                    _run_mission_with_capture(mission_registry[provider], client, provider, timeout),
                    name=f"TritonTask-{provider}",
                )
                for provider in providers
            ]

    # ---------------------------------------------------------------------------
    # 4) Aquí ya SALIMOS del "async with TaskGroup()", por lo que es GARANTÍA que
    #    todas las tareas terminaron (con éxito o con incidente capturado).
    #    Por eso es seguro leer .result() de cada una — no se bloqueará.
    #    NOTA: si alguna tarea hubiera lanzado una excepción NO capturada, el
    #    TaskGroup la habría agregado a un ExceptionGroup y este código no se
    #    alcanzaría. Gracias al wrapper, aquí todas las tareas retornan "limpio".
    # ---------------------------------------------------------------------------
    outcomes: List[MissionOutcome] = [task.result() for task in tasks]
    incidents = [outcome for outcome in outcomes if isinstance(outcome, TritonError)]

    # ---------------------------------------------------------------------------
    # 5) Si hubo incidentes, los RE-ELEVAMOS agrupados en un ExceptionGroup
    #    nativo. Los "sentinela" (excepciones devueltas como valor) se vuelven a
    #    lanzar como un grupo. De esta forma la capa de presentación
    #    (app_operator.py) puede capturarlos quirúrgicamente con "except*".
    # ---------------------------------------------------------------------------
    if incidents:
        raise ExceptionGroup("Incidentes de telemetría detectados por TaskGroup", incidents)

    # Si NO hubo incidentes (o los que hubo ya se lanzaron arriba), devolvemos
    # solo los resultados EXITOSOS (los que no son excepciones Tritón).
    return [outcome for outcome in outcomes if not isinstance(outcome, TritonError)]


# ---------------------------------------------------------------------------
# BLOQUE DE ARRANQUE PARA PRUEBA RÁPIDA (NO usado cuando se importa el paquete)
# ---------------------------------------------------------------------------
# "if __name__ == '__main__':" es el "punto de entrada" estándar de Python.
#   - Cuando OTRO script hace "import triton_telemetry.core", Python ejecuta el
#     archivo completo PERO con __name__ == "triton_telemetry.core" (el nombre
#     del módulo), así que este bloque NO se ejecuta.
#   - Cuando corremos "python core.py" DIRECTAMENTE, Python pone
#     __name__ == "__main__" y este bloque SÍ se ejecuta.
# Es la forma de permitir que un módulo se use tanto como biblioteca importable
# como como script autónomo de prueba.
if __name__ == "__main__":
    # Permite validar la conectividad y el orquestador sin pasar por la CLI
    # completa de app_operator.py.
    import sys
    # sys.argv[1:]: toma los argumentos pasados en la terminal (ej. "python
    # core.py AWS GCP" -> ["AWS", "GCP"]). Si no hay ninguno, probamos los 3.
    providers = sys.argv[1:] if len(sys.argv) > 1 else ["AWS", "Azure", "GCP"]
    print(f"[*] Ejecutando escaneo nominal de prueba: {providers}")
    try:
        # asyncio.run() arranca el event loop, ejecuta la corrutina raíz
        # (scan_all_providers) hasta que termine y después cierra el loop.
        # Es la forma estándar de "correr" una corrutina desde código síncrono.
        resultados = asyncio.run(scan_all_providers(providers, timeout=2.5))
        for r in resultados:
            # Imprimimos cada resultado exitoso: proveedor, latencia en
            # segundos (3 decimales), id del payload y estado.
            print(
                f"  -> {r['provider']}: {r['latency_sec']:.3f}s | "
                f"ID: {r['payload_id']} | {r['status']}"
            )
        print(f"[+] Escaneo completado: {len(resultados)}/{len(providers)} proveedores respondieron.")
    except ExceptionGroup as group:
        # Si scan_all_providers re-eleva incidentes, aquí los capturamos y los
        # mostramos de forma simple para que la prueba no termine con un crash.
        print(f"[!] {len(group.exceptions)} incidente(s) detectado(s):")
        for exc in group.exceptions:
            print(f"    - {exc}")
