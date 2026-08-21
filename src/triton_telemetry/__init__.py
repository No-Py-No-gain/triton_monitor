# Expone la API pública del paquete mediante __all__
# Descripcion: Frontera de imports. Encapsula y declara explícitamente cuáles objetos se exponen cuando un desarrollador hace importación masiva de nuestro paquete.

# src/triton_telemetry/__init__.py
from .exceptions import TritonError, ProviderTimeoutError, CorruptedPayloadError, NetworkPeeringError
from .sanitizer import parse_timeout, parse_cluster_id
from .logging_engine import setup_triton_logging
from .core import scan_all_providers

# Definición rigurosa de la API pública del paquete
__all__ = [
    "TritonError",
    "ProviderTimeoutError",
    "CorruptedPayloadError",
    "NetworkPeeringError",
    "parse_timeout",
    "parse_cluster_id",
    "setup_triton_logging",
    "scan_all_providers"
]