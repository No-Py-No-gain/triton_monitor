# Excepciones semánticas custom de Triton (no BaseException)
# Descripcion: Mapeo semántico de errores. Se evita estrictamente heredar de `BaseException` para no secuestrar señales vitales del sistema operativo (como Ctrl+C).

# src/triton_telemetry/exceptions.py
class TritonError(Exception):
    """Excepción base para todos los fallos del ecosistema TritonMonitor."""
    pass


class ProviderTimeoutError(TritonError):
    """Lanzada cuando un proveedor de nube supera el tiempo de espera (Timeout) establecido."""
    pass


class CorruptedPayloadError(TritonError):
    """Lanzada cuando la respuesta recibida del proveedor cloud es corrupta, no serializable o corresponde a un estatus HTTP fallido (4xx/5xx)."""
    pass


class NetworkPeeringError(TritonError):
    """Lanzada cuando existen fallos de resolución de DNS, ruteo o denegación de conexión física."""
    pass