# =============================================================================
# módulo: exceptions.py — Excepciones semánticas custom de Tritón
# =============================================================================
# ROL EN LA ARQUITECTURA:
#   Este módulo define el VOCABULARIO SEMÁNTICO de errores del ecosistema
#   TritonMonitor. En vez de que cada capa lance errores "crudos" de librerías
#   (httpx.ReadTimeout, httpx.HTTPStatusError, json.JSONDecodeError...), el
#   proyecto traduce todo a excepciones propias del dominio, de modo que la
#   capa de presentación (app_operator.py) pueda capturarlas de forma
#   quirúrgica y tipada.
#
#   JERARQUÍA:  Todo hereda de TritonError, que a su vez hereda de Exception.
#               Siguiendo el requisito formal, NUNCA heredamos de BaseException,
#               porque esa clase raíz también captura señales vitales del
#               sistema operativo (como Ctrl+C / KeyboardInterrupt) que NO
#               deberían interceptarse jamás.
#
#   Este archivo es UNICAMENTE declarativo: define las clases. La lógica que las
#   LANZA vive en core.py (traducción de errores de httpx) y la que las CAPTURA
#   vive en app_operator.py (except* sobre ExceptionGroup).
#
# RELACIONES DEL PAQUETE:
#   - core.py          -> importa estas clases y las lanza con "raise ... from".
#   - app_operator.py  -> las captura con "except* <Tipo> as group:".
#   - __init__.py      -> las re-exporta en la API pública del paquete.
# =============================================================================
# Descripcion: Mapeo semántico de errores. Se evita estrictamente heredar de `BaseException`
# para no secuestrar señales vitales del sistema operativo (como Ctrl+C).

# src/triton_telemetry/exceptions.py


# ---------------------------------------------------------------------------
# Excepción base del dominio
# ---------------------------------------------------------------------------
# Heredar de Exception (no de BaseException) garantiza que nuestras excepciones
# no intercepten KeyboardInterrupt (Ctrl+C), SystemExit u otras señales de
# sistema. Sirve como supertipo común para capturar "cualquier incidente
# Tritón" de forma genérica (lo usa core._run_mission_with_capture).
class TritonError(Exception):
    """Excepción base para todos los fallos del ecosistema TritonMonitor."""
    pass


# ---------------------------------------------------------------------------
# Subclases de dominio (cada una representa UN tipo de incidente)
# ---------------------------------------------------------------------------

class ProviderTimeoutError(TritonError):
    """Lanzada cuando un proveedor de nube supera el tiempo de espera (Timeout) establecido.
    Se origina cuando httpx.TimeoutException estalla en core.py (p.ej. al consultar
    httpbin.org/delay/3 con un timeout menor al retardo del servidor)."""
    pass


class CorruptedPayloadError(TritonError):
    """Lanzada cuando la respuesta recibida del proveedor cloud no cumple con el formato o está corrupta.
    Cubre dos casos en core.py: JSON no serializable / mal formado (json.JSONDecodeError)
    y JSON válido pero fuera del contrato (no es un dict)."""
    pass


class NetworkPeeringError(TritonError):
    """Lanzada cuando existen fallos de resolución de DNS, ruteo o denegación de conexión física (e.g., 4xx, 5xx).
    Se origina por fallos de transporte antes de recibir respuesta (httpx.RequestError)
    o por estatus HTTP erróneos de servidor/cliente (504, 422, 4xx, 5xx)."""
    pass


# ---------------------------------------------------------------------------
# BLOQUE DE ARRANQUE PARA PRUEBA RÁPIDA (NO usado cuando se importa el paquete)
# ---------------------------------------------------------------------------
# Al igual que core.py y sanitizer.py, este bloque solo corre cuando ejecutamos
# "python exceptions.py" directamente. Verifica que la jerarquía de herencia es
# correcta (requisito 1) y que las excepciones se lanzan/capturan/encadenan tal
# y como lo exige el flujo de core.py. Al importar el paquete NO se ejecuta.
if __name__ == "__main__":
    import sys
    fallos = 0

    def chequeo(condicion, descripcion):
        global fallos
        if condicion:
            print(f"[OK]    {descripcion}")
        else:
            print(f"[ERROR] {descripcion}")
            fallos += 1

    print("=== JERARQUÍA DE EXCEPCIONES (requisito 1) ===")
    # TritonError debe heredar de Exception...
    chequeo(issubclass(TritonError, Exception), "TritonError hereda de Exception")
    # ...y debe heredar DIRECTAMENTE de Exception (no de BaseException). Esto
    # evita capturar señales de sistema como Ctrl+C. Ojo: "issubclass(TritonError,
    # BaseException)" daría True porque Exception YA hereda de BaseException; lo
    # que importa es que el padre inmediato (__bases__) sea Exception, no
    # BaseException.
    chequeo(TritonError.__bases__ == (Exception,), "TritonError hereda DIRECTAMENTE de Exception (no de BaseException)")
    # Las tres subclases de dominio deben ser subtipos de TritonError.
    chequeo(issubclass(ProviderTimeoutError, TritonError), "ProviderTimeoutError es TritonError")
    chequeo(issubclass(CorruptedPayloadError, TritonError), "CorruptedPayloadError es TritonError")
    chequeo(issubclass(NetworkPeeringError, TritonError), "NetworkPeeringError es TritonError")
    print()

    print("=== COMPORTAMIENTO (como lo usa core.py) ===")
    try:
        raise ProviderTimeoutError("timeout de prueba")
    except TritonError as e:
        chequeo(isinstance(e, ProviderTimeoutError), "Lanzada y capturada como TritonError")
    else:
        chequeo(False, "Lanzada y capturada como TritonError")

    # Reproduce el patrón "raise ... from" + add_note() de core.py para validar
    # que las notas forenses se adjuntan y sobreviven al encadenamiento.
    try:
        try:
            raise ValueError("causa raíz nativa simulada")
        except ValueError as nativa:
            semantica = NetworkPeeringError("fallo de red de prueba")
            semantica.add_note("nota forense de prueba")
            raise semantica from nativa
    except TritonError as e:
        chequeo(e.__cause__ is not None, "La causa raíz original se conserva (raise ... from)")
        chequeo(hasattr(e, "__notes__") and "nota forense de prueba" in e.__notes__,
                "Las notas forenses se adjuntan con add_note()")
    print()

    print("RESULTADO FINAL:", f"{8 - fallos}/8 correctos.")
    sys.exit(0 if fallos == 0 else 1)