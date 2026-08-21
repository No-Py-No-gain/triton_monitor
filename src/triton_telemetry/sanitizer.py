# Validación de parámetros CLI (argparse)
# Descripcion: Validación declarativa estricta en la frontera. Intercepta datos corruptos o fuera de rango de dominio antes de que interactúen con el bucle de eventos o los hilos de red.

import argparse
import re

def parse_timeout(value: str) -> float:
    """
    Sanitiza y valida el tiempo de espera (timeout) para las peticiones HTTP.
    Debe ser un flotante estrictamente en el rango [0.1, 5.0] segundos.
    """
    try:
        val = float(value)
        if not (0.1 <= val <= 5.0):
            raise ValueError("El timeout debe estar entre 0.1 y 5.0 segundos.")
        return val
    except ValueError as e:
        # argparse requiere ArgumentTypeError para disparar el mensaje de ayuda y salir con código 2
        raise argparse.ArgumentTypeError(f"Timeout inválido '{value}': {str(e)}")


def parse_cluster_id(value: str) -> str:
    """
    Valida que el identificador del clúster siga el patrón formal de expresión regular:
    cluster-<region>-<numero_dos_digitos> (e.g., cluster-us-east-01).
    """
    pattern = r"^cluster-[a-z]{2,10}-[a-z]+-\d{2}$"
    if not re.match(pattern, value):
        raise argparse.ArgumentTypeError(
            f"El ID del clúster '{value}' no cumple con el formato requerido "
            f"(ejemplo válido: 'cluster-us-east-01')."
        )
    return value