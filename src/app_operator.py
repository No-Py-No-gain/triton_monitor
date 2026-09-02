# Punto de entrada CLI ejecutable (argparse + except*)
# Descripcion: El punto de entrada consumidor de la CLI. Implementa el orquestador principal, inyecta los sanitizadores en argparse, efectúa la captura quirúrgica concurrentes con except* y libera recursos en finally bajo la norma PEP 765.

# src/app_operator.py
import sys
import argparse
import asyncio
import logging
from triton_telemetry import (
    setup_triton_logging,
    scan_all_providers,
    parse_timeout,
    parse_cluster_id,
    ProviderTimeoutError,
    NetworkPeeringError,
    CorruptedPayloadError,
    TritonError
)

logger = setup_triton_logging()

# Blindaje de consola Windows: cp1252 no puede codificar los glifos del pipeline forense
if sys.stdout.encoding and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    sys.stdout.reconfigure(encoding="utf-8")

def build_cli_parser() -> argparse.ArgumentParser:
    """Configura el analizador CLI oficial conforme a las reglas UPATECO."""
    parser = argparse.ArgumentParser(
        prog="TritonMonitor",
        description="Consola de Telemetría Multicloud y Observabilidad Asíncrona (PROYECTO TRITÓN)."
    )
    
    # Argumento posicional obligatorio: Lista de proveedores cloud a monitorear (ej. AWS Azure GCP)
    parser.add_argument(
        "proveedores",
        nargs="+",
        choices=["AWS", "Azure", "GCP"],
        help="Lista de identificadores de los proveedores cloud a monitorear."
    )
    
    # Argumento obligatorio: ID de clúster con sanitizador de formato personalizado
    parser.add_argument(
        "-c", "--cluster-id",
        type=parse_cluster_id,
        required=True,
        help="Identificador único del clúster (formato: cluster-<region>-<numero_dos_digitos>)."
    )
    
    # Argumento opcional: Tiempo de espera (timeout) con sanitizador personalizado
    parser.add_argument(
        "-t", "--timeout",
        type=parse_timeout,
        default=2.5,
        help="Tiempo de espera límite para las peticiones HTTP (0.1s - 5.0s)."
    )
    
    # Bandera opcional: Forzar inyección de Caos real para pruebas
    parser.add_argument(
        "--chaos",
        action="store_true",
        help="Forzar inyección de caos probabilístico en las APIs de nube reales."
    )
    
    # Restricción de dominio: Modos operativos
    parser.add_argument(
        "-m", "--mode",
        choices=["nominal", "debug", "emergency"],
        default="nominal",
        help="Modo de operación del despachador de telemetría."
    )

    # Grupo opcional mutuamente excluyente: salida de texto a stdout (sys.stdout)
    # --verbose y --quiet no pueden coexistir; controlan el nivel del handler consola.
    output_group = parser.add_mutually_exclusive_group(required=False)
    output_group.add_argument(
        "--verbose",
        action="store_true",
        help="Salida detallada a stdout: incluye notas forenses y registros DEBUG."
    )
    output_group.add_argument(
        "--quiet",
        action="store_true",
        help="Salida mínima a stdout: solo WARNING y errores críticos."
    )

    return parser


async def async_main():
    parser = build_cli_parser()
    args = parser.parse_args()

    # Ajuste dinámico del nivel del handler stdout (sys.stdout) según grupo excluyente.
    # El dictConfig deja stdout_console en INFO por defecto; aquí lo refinamos en runtime.
    # El handler real vive dentro del QueueListener (logger.handlers solo tiene el
    # QueueHandler de desacople no bloqueante), por eso accedemos via logger.listener.
    if hasattr(logger, "listener") and logger.listener:
        for h in logger.listener.handlers:
            # Identificar el handler de consola (StreamHandler puro, no FileHandler)
            if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler):
                if args.verbose:
                    h.setLevel(logging.DEBUG)
                elif args.quiet:
                    h.setLevel(logging.WARNING)
                # else: se respeta el nivel INFO por defecto del dictConfig
                break  # stdout_console es único

    logger.info("=" * 64)
    logger.info(f"  INICIANDO MONITOREO MULTICLOUD: PROYECTO TRITÓN")
    logger.info("=" * 64)
    logger.info(f"  Clúster Objetivo: {args.cluster_id}")
    logger.info(f"  Modo Operativo: {args.mode.upper()}")
    logger.info(f" Proveedores seleccionados: {', '.join(args.proveedores)}")
    logger.info(f" Timeout límite configurado: {args.timeout}s")
    if args.chaos:
        logger.warning(" ADVERTENCIA: MODO CAOS ACTIVADO. Se inyectarán fallos reales de red.")
    logger.info("=" * 64)

    try:
        # Lanzamos el proceso asíncrono concurrente (TaskGroup)
        results = await scan_all_providers(args.proveedores, args.timeout, use_chaos=args.chaos)
        
        logger.info("\n ESCANEO COMPLETADO CON ÉXITO SIN ANOMALÍAS:")
        for r in results:
            logger.info(f"  • {r['provider']} -> Latencia de Red: {r['latency_sec']:.3f}s | ID de Evento: {r['payload_id']} | Estado: {r['status']}")
            
    except* ProviderTimeoutError as group:
        # Captura Quirúrgica 1: Tiempos de espera de proveedores agotados (timeout real de la API)
        logger.error(f"\n ANOMALÍA: DETECTADOS TIMEOUTS EN PROVEEDORES CLOUD ({len(group.exceptions)} incidentes):")
        for exc in group.exceptions:
            logger.error(f"   Fallo: {exc}", exc_info=exc)
            # Mostrar notas de diagnóstico dinámico (add_note)
            for note in getattr(exc, "__notes__", []):
                logger.error(f"     └─ [FORENSE TRITÓN] {note}")
                
    except* CorruptedPayloadError as group:
        # Captura Quirúrgica 2: Mitigar códigos de error HTTP de forma lógica sin detener el programa
        # Patrón de plantilla §4.3: prefijo ADVERTENCIA con doble espacio;
        # el texto extendido "O ESTATUS HTTP FALLIDOS" refleja el mapeo
        # §2.2.2 (estatus HTTP → CorruptedPayloadError).
        logger.error(f"\n  ADVERTENCIA: RECIBIDOS PAYLOADS DE TELEMETRÍA CORRUPTOS O ESTATUS HTTP FALLIDOS ({len(group.exceptions)} incidentes):")
        for exc in group.exceptions:
            logger.error(f"   Fallo: {exc}", exc_info=exc)
            for note in getattr(exc, "__notes__", []):
                logger.error(f"     └─ [FORENSE TRITÓN] {note}")
                
    except* NetworkPeeringError as group:
        # Captura Quirúrgica 3: Fallos catastróficos de DNS/conexión cuando el host no tiene internet
        logger.error(f"\n ANOMALÍA: DETECTADOS FALLOS FÍSICOS DE CONEXIÓN O ROUTING ({len(group.exceptions)} incidentes):")
        for exc in group.exceptions:
            logger.error(f"   Fallo: {exc}", exc_info=exc)
            for note in getattr(exc, "__notes__", []):
                logger.error(f"     └─ [FORENSE TRITÓN] {note}")
                
    except* TritonError as group:
        # Captura Quirúrgica 4: Fallos genéricos de Tritón no catalogados
        logger.error("\n DETECTADO ERROR OPERACIONAL IMPREVISTO EN ECOSISTEMA TRITÓN:")
        for exc in group.exceptions:
            logger.error(f"   Fallo: {exc}", exc_info=exc)
            for note in getattr(exc, "__notes__", []):
                logger.error(f"     └─ [FORENSE TRITÓN] {note}")

    finally:
        # PEP 765 / Python 3.14: finally solo se usa para liberar descriptores y hilos
        # NUNCA inyectar un 'return', 'break' o 'continue' aquí, o silenciarás las excepciones residuales
        logger.info("\n" + "=" * 64)
        logger.info("  [FIN DE CICLO] Recursos liberados de la Operación Tritón.")
        logger.info("=" * 64)
        
        # Detener de manera ordenada el despachador no bloqueante QueueListener
        if hasattr(logger, "listener") and logger.listener:
            logger.listener.stop()


if __name__ == "__main__":
    asyncio.run(async_main())