# Formateador JSON avanzado y pipeline asincrono no bloqueante
#
# Responsabilidad:
# - Formatear LogRecord como JSON estructurado.
# - Serializar excepciones y ExceptionGroup de forma recursiva.
# - Preservar notes y causas encadenadas.
# - Desacoplar el logging mediante QueueHandler + QueueListener.
# - Rotar archivos al alcanzar 2 MiB.
# - Mantener como máximo 3 archivos históricos.
# - Comprimir automáticamente los históricos mediante gzip.

from __future__ import annotations

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
    Comprime el archivo rotado a formato gzip.

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
        # Esto permite que os.replace() sea atómico en el
        # mismo sistema de archivos.
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
        #
        # El .tmp pasa a ser el .gz definitivo.
        # ----------------------------------------------------

        os.replace(
            temporary_path,
            destination_path,
        )

        temporary_path = None

        # ----------------------------------------------------
        # El original se elimina SOLO después de que el gzip
        # fue creado correctamente.
        # ----------------------------------------------------

        source_path.unlink()

    except Exception:
        # ----------------------------------------------------
        # Si algo falla, intentamos eliminar solamente el
        # temporal. El archivo original permanece intacto.
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

        # ----------------------------------------------------
        # ExceptionGroup
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

        payload: dict[str, Any] = {
            "timestamp": (
                dt_utc.isoformat()
            ),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "task_name": getattr(
                record,
                "taskName",
                None,
            ),
            "thread_name": record.threadName,
            "filename": record.filename,
            "line": record.lineno,
        }

        # ----------------------------------------------------
        # Excepción
        # ----------------------------------------------------

        if record.exc_info:
            _, exc_value, _ = record.exc_info

            if exc_value is not None:
                payload["exception"] = (
                    self._serialize_exception(
                        exc_value
                    )
                )

                payload["stack_trace"] = (
                    self.formatException(
                        record.exc_info
                    )
                )

        # ----------------------------------------------------
        # Campos personalizados enviados mediante extra={}
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
        }

        for key, value in record.__dict__.items():

            if (
                key not in reserved_fields
                and not key.startswith("_")
            ):
                payload[key] = (
                    self._serialize_value(value)
                )

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
        QueueHandler
            |
            v
        queue.Queue
            |
            v
        QueueListener
            |
            v
        AsyncJSONFormatter
            |
            v
        RotatingFileHandler
            |
            v
        gzip
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
    # Configuración declarativa.
    # --------------------------------------------------------

    logging_schema = {
        "version": 1,
        "disable_existing_loggers": False,

        "formatters": {
            "json_structured": {
                "()": AsyncJSONFormatter,
            },

            "console_clean": {
                "format": (
                    "%(asctime)s "
                    "[%(levelname)s] "
                    "[%(name)s] "
                    "%(message)s"
                ),
                "datefmt": "%H:%M:%S",
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
    # Aplicar configuración.
    # --------------------------------------------------------

    logging.config.dictConfig(
        logging_schema
    )

    logger = logging.getLogger(
        LOGGER_NAME
    )

    # --------------------------------------------------------
    # Buscar el RotatingFileHandler configurado.
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
    # Inyectar callbacks de gzip.
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
    # QueueHandler:
    #
    # El logger deja el LogRecord en memoria.
    # --------------------------------------------------------

    queue_handler = (
        logging.handlers.QueueHandler(
            log_queue
        )
    )

    queue_handler.setLevel(
        logging.DEBUG
    )

    # --------------------------------------------------------
    # Guardamos los handlers físicos originales.
    #
    # El QueueListener será el encargado de utilizarlos.
    # --------------------------------------------------------

    real_handlers = list(
        logger.handlers
    )

    # --------------------------------------------------------
    # QueueListener:
    #
    # Un hilo secundario consume la cola y ejecuta los
    # handlers físicos.
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
    # Ahora solamente entrega LogRecords al QueueHandler.
    # --------------------------------------------------------

    logger.handlers = [
        queue_handler
    ]

    # --------------------------------------------------------
    # Arrancar el hilo consumidor.
    # --------------------------------------------------------

    listener.start()

    # --------------------------------------------------------
    # Guardamos referencias para permitir que app_operator.py
    # pueda detener correctamente el listener.
    # --------------------------------------------------------

    logger.listener = listener
    logger.queue_handler = queue_handler
    logger.log_queue = log_queue

    return logger
