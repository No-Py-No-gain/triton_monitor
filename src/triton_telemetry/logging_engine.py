# src/triton_telemetry/logging_engine.py

"""
Motor de logging estructurado y asíncrono del Proyecto Tritón.

Responsabilidades:
- Formatear LogRecord como JSON estructurado.
- Serializar excepciones y ExceptionGroup de forma recursiva.
- Preservar notes, causas y contextos encadenados.
- Desacoplar el logging mediante QueueHandler + QueueListener.
- Rotar archivos al alcanzar 2 MiB.
- Mantener como máximo 3 archivos históricos.
- Comprimir automáticamente los históricos mediante GZIP.

Integrante 3:
    - AsyncJSONFormatter.
    - Serialización recursiva de ExceptionGroup.
    - Conservación de causas (__cause__) y notas (__notes__).
    - Mapeo dinámico de metadata de LogRecord y extra={...}.

Integrante 4:
    - QueueHandler + QueueListener.
    - RotatingFileHandler.
    - Rotación de archivos.
    - Compresión GZIP.
"""

from __future__ import annotations

import copy
import gzip
import json
import logging
import logging.config
import logging.handlers
import os
import queue
import shutil
import tempfile

from datetime import datetime, timezone
from pathlib import Path
from typing import Any


# ============================================================
# CONSTANTES
# ============================================================

MAX_LOG_BYTES = 2 * 1024 * 1024
BACKUP_COUNT = 3

LOGGER_NAME = "triton_monitor"


# ============================================================
# GZIP ROTATION
# ============================================================

def gzip_namer(name: str) -> str:
    """
    Modifica el nombre generado por RotatingFileHandler
    agregando la extensión .gz.

    Ejemplo:

        triton_services.log.1
        ->
        triton_services.log.1.gz
    """

    return f"{name}.gz"


def gzip_rotator(source: str, dest: str) -> None:
    """
    Comprime el archivo rotado a formato GZIP.

    La compresión se realiza primero sobre un archivo temporal.
    El archivo definitivo se reemplaza de forma atómica.

    El archivo original solamente se elimina después de
    comprobar que la compresión terminó correctamente.
    """

    source_path = Path(source)
    destination_path = Path(dest)

    destination_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary_path: Path | None = None

    try:
        # ----------------------------------------------------
        # Crear archivo temporal en el mismo directorio.
        #
        # Esto permite que os.replace() sea atómico dentro
        # del mismo sistema de archivos.
        # ----------------------------------------------------

        fd, temp_name = tempfile.mkstemp(
            prefix=f".{destination_path.name}.",
            suffix=".tmp",
            dir=destination_path.parent,
        )

        os.close(fd)

        temporary_path = Path(temp_name)

        # ----------------------------------------------------
        # Comprimir
        # ----------------------------------------------------

        with source_path.open("rb") as source_file:
            with gzip.open(
                temporary_path,
                "wb",
                compresslevel=9,
            ) as destination_file:

                shutil.copyfileobj(
                    source_file,
                    destination_file,
                )

        # ----------------------------------------------------
        # Reemplazo atómico.
        # ----------------------------------------------------

        os.replace(
            temporary_path,
            destination_path,
        )

        temporary_path = None

        # ----------------------------------------------------
        # El original se elimina SOLO después de que el GZIP
        # fue creado correctamente.
        # ----------------------------------------------------

        source_path.unlink()

    except Exception:

        # ----------------------------------------------------
        # Si algo falla, eliminamos solamente el temporal.
        # El archivo original permanece intacto.
        # ----------------------------------------------------

        if temporary_path is not None:
            try:
                temporary_path.unlink(
                    missing_ok=True
                )
            except OSError:
                pass

        raise


# ============================================================
# QUEUE HANDLER
# ============================================================

class PreservingQueueHandler(logging.handlers.QueueHandler):
    """
    QueueHandler que conserva la información de excepciones.

    QueueHandler.prepare() normalmente prepara el LogRecord
    antes de colocarlo en la cola y puede eliminar exc_info.

    AsyncJSONFormatter necesita exc_info para construir:

        - exception_tree
        - stack_trace

    Como el proyecto utiliza queue.Queue dentro del mismo
    proceso, podemos conservar esos objetos.

    Se devuelve una copia del LogRecord para no modificar
    el registro original.
    """

    def prepare(
        self,
        record: logging.LogRecord,
    ) -> logging.LogRecord:

        return copy.copy(record)


# ============================================================
# CONSOLE FORMATTER
# ============================================================

# Línea única que reemplaza al traceback crudo en consola: el árbol
# forense completo (exception_tree + stack_trace) persiste únicamente
# en el log JSON (triton_services.log).
TRACEBACK_OMITIDO = (
    "[traceback omitido en consola — "
    "árbol forense completo en triton_services.log]"
)

CONSOLE_FORMAT = "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
CONSOLE_DATEFMT = "%H:%M:%S"


class ConsoleFormatter(logging.Formatter):
    """
    Formatter de consola que omite los tracebacks crudos.

    ``formatException()`` devuelve una única línea informativa en
    lugar del traceback multi-línea, de modo que la consola del
    operador queda legible. El ``RotatingFileHandler`` (con
    ``AsyncJSONFormatter``) sigue persistiendo el árbol forense
    completo — ``exception_tree`` y ``stack_trace`` — en
    ``triton_services.log``.

    Solo cambia el render de consola: la propagación de ``exc_info``
    a través de la cola (``PreservingQueueHandler``) y el pipeline
    JSON permanecen intactos.
    """

    def __init__(
        self,
        format: str = CONSOLE_FORMAT,
        datefmt: str = CONSOLE_DATEFMT,
    ) -> None:

        # ``format`` (en lugar de ``fmt``) refleja el nombre de la clave
        # declarativa del esquema dictConfig, que inyecta las claves
        # restantes como kwargs al callable custom (rol del Integrante 5
        # §2.2.5); acá se traduce al parámetro canónico ``fmt`` de
        # logging.Formatter. Las constantes de módulo quedan como defaults.
        super().__init__(
            fmt=format,
            datefmt=datefmt,
        )

    def formatException(self, exc_info) -> str:
        """
        Reemplaza el traceback crudo por la línea de omisión.
        """

        return TRACEBACK_OMITIDO


# ============================================================
# JSON FORMATTER
# ============================================================

class AsyncJSONFormatter(logging.Formatter):
    """
    Formatter JSON estructurado para el sistema de observabilidad.

    Se ejecuta dentro del QueueListener, por lo que el trabajo
    de formateo y escritura queda desacoplado del hilo que
    genera originalmente el LogRecord.
    """

    def _serialize_exception(
        self,
        exc: BaseException,
    ) -> dict[str, Any]:
        """
        Serializa recursivamente una excepción.

        Soporta:

        - excepciones normales
        - ExceptionGroup
        - BaseExceptionGroup
        - add_note()
        - raise ... from ...
        - excepciones encadenadas
        """

        exception_data: dict[str, Any] = {
            "class": exc.__class__.__name__,
            "message": str(exc),
        }

        # ----------------------------------------------------
        # Notas dinámicas agregadas mediante add_note()
        # ----------------------------------------------------

        notes = getattr(
            exc,
            "__notes__",
            None,
        )

        if notes:
            exception_data["notes"] = list(notes)
        else:
            exception_data["notes"] = []

        # ----------------------------------------------------
        # ExceptionGroup / BaseExceptionGroup
        # ----------------------------------------------------

        if isinstance(
            exc,
            BaseExceptionGroup,
        ):
            exception_data["nested_exceptions"] = [
                self._serialize_exception(child)
                for child in exc.exceptions
            ]

        # ----------------------------------------------------
        # Causa explícita:
        #
        # raise NewError(...) from original_error
        # ----------------------------------------------------

        if exc.__cause__ is not None:

            exception_data["cause"] = (
                self._serialize_exception(
                    exc.__cause__
                )
            )

        # ----------------------------------------------------
        # Contexto implícito de excepción.
        #
        # Solo se agrega cuando no existe una causa explícita.
        # ----------------------------------------------------

        elif (
            exc.__context__ is not None
            and not exc.__suppress_context__
        ):

            exception_data["context_exception"] = (
                self._serialize_exception(
                    exc.__context__
                )
            )

        return exception_data

    # --------------------------------------------------------

    def _serialize_value(
        self,
        value: Any,
    ) -> Any:
        """
        Convierte valores adicionales a estructuras compatibles
        con JSON.
        """

        if value is None:
            return None

        if isinstance(
            value,
            (
                str,
                int,
                float,
                bool,
            ),
        ):
            return value

        if isinstance(value, dict):
            return {
                str(key): self._serialize_value(item)
                for key, item in value.items()
            }

        if isinstance(
            value,
            (
                list,
                tuple,
                set,
            ),
        ):
            return [
                self._serialize_value(item)
                for item in value
            ]

        return str(value)

    # --------------------------------------------------------

    def format(
        self,
        record: logging.LogRecord,
    ) -> str:
        """
        Convierte un LogRecord completo en una línea JSON.
        """

        # ----------------------------------------------------
        # Timestamp UTC ISO-8601
        # ----------------------------------------------------

        dt_utc = datetime.fromtimestamp(
            record.created,
            tz=timezone.utc,
        )

        task_name = getattr(
            record,
            "taskName",
            None,
        )

        payload: dict[str, Any] = {
            "timestamp": (
                dt_utc
                .isoformat()
                .replace("+00:00", "Z")
            ),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "process": record.process,
            "thread_name": record.threadName,
            "task_name": task_name,

            # Se conserva también async_task por compatibilidad
            # con la implementación original del formatter.
            "async_task": task_name,

            "filename": record.filename,
            "line": record.lineno,
        }

        # ----------------------------------------------------
        # Excepciones
        # ----------------------------------------------------

        if record.exc_info:

            _, exc_value, _ = record.exc_info

            if exc_value is not None:

                serialized_exception = (
                    self._serialize_exception(
                        exc_value
                    )
                )

                # Nombre usado por la nueva implementación.
                payload["exception"] = (
                    serialized_exception
                )

                # Nombre utilizado originalmente por
                # AsyncJSONFormatter.
                payload["exception_tree"] = (
                    serialized_exception
                )

                payload["stack_trace"] = (
                    self.formatException(
                        record.exc_info
                    )
                )

        # ----------------------------------------------------
        # Campos internos estándar de LogRecord
        # ----------------------------------------------------

        reserved_fields = {
            "name",
            "msg",
            "args",
            "levelname",
            "levelno",
            "pathname",
            "filename",
            "module",
            "exc_info",
            "exc_text",
            "stack_info",
            "lineno",
            "funcName",
            "created",
            "msecs",
            "relativeCreated",
            "thread",
            "threadName",
            "processName",
            "process",
            "message",
            "taskName",
            "asctime",
        }

        # ----------------------------------------------------
        # Campos personalizados enviados mediante extra={}
        # ----------------------------------------------------

        for key, value in record.__dict__.items():

            if (
                key not in reserved_fields
                and not key.startswith("_")
            ):

                payload[key] = (
                    self._serialize_value(
                        value
                    )
                )

        # ----------------------------------------------------
        # Conversión final a JSON
        # ----------------------------------------------------

        return json.dumps(
            payload,
            ensure_ascii=False,
            default=str,
        )


# ============================================================
# SETUP PRINCIPAL
# ============================================================

def setup_triton_logging(
    log_filename: str = "triton_services.log",
) -> logging.Logger:
    """
    Configura el pipeline completo de logging de Tritón.

    Arquitectura:

        Logger
          |
          v
    PreservingQueueHandler
          |
          v
      queue.Queue
          |
          v
     QueueListener
       /       \
      v         v
   Consola   RotatingFileHandler
                  |
                  v
          AsyncJSONFormatter
                  |
                  v
                GZIP
    """

    # --------------------------------------------------------
    # Evitar configurar dos veces el sistema.
    # --------------------------------------------------------

    existing_logger = logging.getLogger(
        LOGGER_NAME
    )

    existing_listener = getattr(
        existing_logger,
        "listener",
        None,
    )

    if existing_listener is not None:
        return existing_logger

    # --------------------------------------------------------
    # Configuración declarativa
    # --------------------------------------------------------

    logging_schema = {
        "version": 1,
        "disable_existing_loggers": False,

        "formatters": {

            "json_structured": {
                "()": AsyncJSONFormatter,
            },

            "console_clean": {
                # Formatter de consola custom: declarativo como el resto del esquema
                # (rol del Integrante 5 §2.2.5: dictConfig declarativo completo),
                # omite tracebacks crudos en consola pero mantiene el prefijo estándar.
                "()": ConsoleFormatter,
                "format": CONSOLE_FORMAT,
                "datefmt": CONSOLE_DATEFMT,
            },
        },

        "handlers": {

            "stdout_console": {
                "class": "logging.StreamHandler",
                "level": "INFO",
                "formatter": "console_clean",
                "stream": "ext://sys.stdout",
            },

            "rotating_file": {
                "class": (
                    "logging.handlers."
                    "RotatingFileHandler"
                ),
                "level": "DEBUG",
                "formatter": "json_structured",

                "filename": log_filename,

                "maxBytes": MAX_LOG_BYTES,
                "backupCount": BACKUP_COUNT,

                "encoding": "utf-8",
            },
        },

        "loggers": {

            LOGGER_NAME: {
                "level": "DEBUG",

                "handlers": [
                    "stdout_console",
                    "rotating_file",
                ],

                "propagate": False,
            },
        },
    }

    # --------------------------------------------------------
    # Aplicar configuración
    # --------------------------------------------------------

    logging.config.dictConfig(
        logging_schema
    )

    logger = logging.getLogger(
        LOGGER_NAME
    )

    # --------------------------------------------------------
    # Buscar RotatingFileHandler configurado
    # --------------------------------------------------------

    file_handler = next(
        (
            handler

            for handler in logger.handlers

            if isinstance(
                handler,
                logging.handlers.RotatingFileHandler,
            )
        ),
        None,
    )

    if file_handler is None:

        raise RuntimeError(
            "No se pudo encontrar "
            "el RotatingFileHandler"
        )

    # --------------------------------------------------------
    # Inyectar callbacks de GZIP
    # --------------------------------------------------------

    file_handler.namer = gzip_namer
    file_handler.rotator = gzip_rotator

    # --------------------------------------------------------
    # Crear cola thread-safe.
    #
    # maxsize=0 significa que la cola no tiene un límite
    # artificial de capacidad.
    # --------------------------------------------------------

    log_queue: queue.Queue[
        logging.LogRecord
    ] = queue.Queue(
        maxsize=0
    )

    # --------------------------------------------------------
    # QueueHandler
    #
    # IMPORTANTE:
    #
    # Se utiliza PreservingQueueHandler para que exc_info
    # llegue intacto al AsyncJSONFormatter.
    # --------------------------------------------------------

    queue_handler = (
        PreservingQueueHandler(
            log_queue
        )
    )

    queue_handler.setLevel(
        logging.DEBUG
    )

    # --------------------------------------------------------
    # Guardar handlers físicos originales.
    #
    # QueueListener será el encargado de utilizarlos.
    # --------------------------------------------------------

    real_handlers = list(
        logger.handlers
    )

    # --------------------------------------------------------
    # QueueListener
    # --------------------------------------------------------

    listener = (
        logging.handlers.QueueListener(
            log_queue,
            *real_handlers,
            respect_handler_level=True,
        )
    )

    # --------------------------------------------------------
    # El logger deja de escribir directamente.
    #
    # Ahora solamente entrega LogRecords a la cola.
    # --------------------------------------------------------

    logger.handlers = [
        queue_handler
    ]

    # --------------------------------------------------------
    # Arrancar hilo consumidor
    # --------------------------------------------------------

    listener.start()

    # --------------------------------------------------------
    # Guardar referencias.
    #
    # Esto permite que otros módulos puedan detener
    # correctamente el listener.
    # --------------------------------------------------------

    logger.listener = listener
    logger.queue_handler = queue_handler
    logger.log_queue = log_queue

    return logger
