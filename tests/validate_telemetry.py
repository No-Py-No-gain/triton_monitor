"""Sub-punto 2 — Validador de Telemetria JSON para Triton Monitor.

Standalone forensic script that walks a log directory, parses every JSON line
emitted by ``AsyncJSONFormatter`` (active ``triton_services.log`` and rotated
``triton_services.log.*.gz``), and verifies structural + forensic integrity.

Usage::

    python3 tests/validate_telemetry.py                 # default: project root
    python3 tests/validate_telemetry.py /path/to/logs   # explicit log dir
    python3 tests/validate_telemetry.py --self-test     # bundled samples check

The module is importable: ``build_report`` and ``validate_entry`` are pure
functions usable from other tooling.

Design notes
------------
* ``exception_tree`` and ``stack_trace`` are OPTIONAL in general but NORMAL
  on the ``except*`` ERROR entries. ``app_operator.py`` passes ``exc_info=exc``
  when logging each incident of the group (since commit bc9e097), and
  ``PreservingQueueHandler.prepare()`` (PR #5, commit bdd3c0e) keeps that
  ``exc_info`` alive across the logging queue, so the recursive exception
  tree DOES reach the JSON payload — the formatter emits it under both the
  ``exception`` and ``exception_tree`` keys (the unification is tracked as
  issue I-16 and must land together with this validator). They stay optional
  here because only the ``except*`` ERROR entries carry them (INFO/DEBUG
  records and the wrapper's "Incidente registrado ..." ERROR records never
  do); when present, the documented shape is fully validated below. The
  ``message`` scan for status codes / forensic notes is kept as
  defense-in-depth and for historical (pre-PR #5) logs whose trees did not
  survive the queue.
* ``asctime`` no longer leaks into the JSON: PR #5 added it to the
  formatter's ``reserved_fields`` (logging_engine.py), so current runs emit
  zero ``asctime`` keys. The counter below only fires on historical
  (pre-PR #5) logs and is reported as metadata, never required.

Exit codes: 0 if every entry is valid and gzip decompression is intact,
           1 if any violation or corruption is found (or no logs found).
"""
from __future__ import annotations

import argparse
import gzip
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
REQUIRED_FIELDS: tuple[str, ...] = (
    "timestamp",
    "level",
    "message",
    "logger",
    "async_task",
    "thread_name",
    "filename",
    "line",
)
VALID_LEVELS: frozenset[str] = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})

# ``async_task`` mirrors ``LogRecord.taskName``, which only exists from
# Python 3.12 (CPython gh-97263). On 3.11 — the documented minimum — the
# formatter emits ``task_name``/``async_task`` as null, so demanding a
# non-empty value there would flag EVERY entry as invalid and flip the whole
# log's verdict to FALLO. The field must still be PRESENT on every
# interpreter; only its non-emptiness is enforced from 3.12 onwards
# (issue I-18, option a — the key name itself stays as-is because the
# formatter/validator key unification is issue I-16, another lane).
_TASK_KEY_MUST_BE_NON_EMPTY = sys.version_info >= (3, 12)

ACTIVE_LOG_NAME = "triton_services.log"

# A 3-digit HTTP status (100-599). Word-bounded so "id 200" style tokens in
# nominal messages do not false-positive (we still cross-check context below).
_HTTP_STATUS_RE = re.compile(r"\b([1-5]\d{2})\b")
# Structured note: "HTTP_Status_Code: 504"
_HTTP_STATUS_NOTE_RE = re.compile(r"HTTP_Status_Code:\s*(\d{3})", re.IGNORECASE)
# Spanish narrative form used by app_operator.py: "Estatus HTTP: 504."
_ESTATUS_HTTP_RE = re.compile(r"Estatus\s*HTTP:?\s*(\d{3})", re.IGNORECASE)
# HTTP verb evidence (issue I-20). httpx >= 0.28 dropped the trailing verb
# from HTTPStatusError messages ("... for url '...'" with no "GET"), so the
# uppercase token only survives in old-format logs; on current logs the verb
# is evidenced by the traceback call site (``await client.get(...)``), which
# the alternation below also accepts. The counter is informational — it
# never affects the verdict.
_HTTP_VERB_RE = re.compile(r"\bGET\b|\bclient\.get\b")


# ---------------------------------------------------------------------------
# Data containers
# ---------------------------------------------------------------------------
@dataclass
class EntryViolation:
    path: str
    line_no: int
    errors: list[str]


@dataclass
class FileReport:
    path: str
    kind: str  # "active" | "gz"
    total_lines: int = 0
    valid_entries: int = 0
    invalid_entries: int = 0
    gzip_ok: bool = True
    gzip_error: str | None = None
    violations: list[EntryViolation] = field(default_factory=list)


@dataclass
class TelemetryReport:
    log_dir: str
    files: list[FileReport] = field(default_factory=list)
    total_entries: int = 0
    valid_entries: int = 0
    invalid_entries: int = 0
    levels: dict[str, int] = field(default_factory=dict)
    exception_classes: dict[str, int] = field(default_factory=dict)
    http_status_codes: dict[int, int] = field(default_factory=dict)
    get_verb_seen: int = 0
    providers_seen: dict[str, int] = field(default_factory=dict)
    asctime_present: int = 0
    stack_trace_present: int = 0
    exception_tree_present: int = 0

    @property
    def all_gzip_ok(self) -> bool:
        return all(f.gzip_ok for f in self.files)

    @property
    def has_violations(self) -> bool:
        return self.invalid_entries > 0 or not self.all_gzip_ok


# ---------------------------------------------------------------------------
# Log discovery + line readers
# ---------------------------------------------------------------------------
def find_log_files(log_dir: Path) -> list[tuple[Path, str]]:
    """Return ``[(path, kind)]`` for the active log and any rotated ``.gz``.

    ``kind`` is ``"active"`` for the uncompressed log or ``"gz"`` for rotated
    archives matching ``triton_services.log.*.gz``.
    """
    found: list[tuple[Path, str]] = []
    active = log_dir / ACTIVE_LOG_NAME
    if active.is_file():
        found.append((active, "active"))
    for gz in sorted(log_dir.glob(f"{ACTIVE_LOG_NAME}.*.gz")):
        found.append((gz, "gz"))
    return found


def iter_lines(path: Path, kind: str) -> Iterator[str]:
    """Yield lines from a plain or gzip-compressed log file.

    Raises ``OSError`` / ``gzip.BadGzipFile`` upstream so the caller can record
    a corruption violation.
    """
    if kind == "gz":
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            for line in fh:
                yield line
    else:
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                yield line


# ---------------------------------------------------------------------------
# Forensic extraction helpers
# ---------------------------------------------------------------------------
def _extract_http_status_codes(obj: dict[str, Any]) -> list[int]:
    """Find HTTP status codes across the structured and narrative surfaces."""
    codes: list[int] = []

    # 1. Structured notes inside exception_tree.
    et = obj.get("exception_tree")
    if isinstance(et, dict):
        for note in et.get("notes", []) or []:
            for m in _HTTP_STATUS_NOTE_RE.finditer(str(note)):
                codes.append(int(m.group(1)))
        cause = et.get("cause")
        if isinstance(cause, dict):
            for m in _HTTP_STATUS_RE.finditer(str(cause.get("message", ""))):
                codes.append(int(m.group(1)))

    # 2. Narrative message (current app_operator.py reality).
    message = str(obj.get("message", ""))
    for m in _ESTATUS_HTTP_RE.finditer(message):
        codes.append(int(m.group(1)))
    for m in _HTTP_STATUS_NOTE_RE.finditer(message):
        codes.append(int(m.group(1)))

    # Deduplicate while preserving order.
    seen: set[int] = set()
    unique: list[int] = []
    for c in codes:
        if c not in seen:
            seen.add(c)
            unique.append(c)
    return unique


def _get_verb_seen(obj: dict[str, Any]) -> bool:
    """True if HTTP verb evidence (``GET`` or the ``client.get`` call site) appears."""
    surfaces = [str(obj.get("message", "")), str(obj.get("stack_trace", ""))]
    et = obj.get("exception_tree")
    if isinstance(et, dict):
        cause = et.get("cause")
        if isinstance(cause, dict):
            surfaces.append(str(cause.get("message", "")))
    return any(_HTTP_VERB_RE.search(s) for s in surfaces)


# ---------------------------------------------------------------------------
# Entry validation
# ---------------------------------------------------------------------------
def validate_entry(obj: dict[str, Any]) -> tuple[bool, list[str]]:
    """Validate a single parsed JSON log entry.

    Returns ``(valid, errors)``. ``exception_tree`` / ``stack_trace`` are only
    checked when present (they are optional in the current emission model).
    """
    errors: list[str] = []

    # Required fields ------------------------------------------------------
    for key in REQUIRED_FIELDS:
        if key not in obj:
            errors.append(f"missing required field: {key!r}")
        elif obj[key] in (None, ""):
            if key == "async_task" and not _TASK_KEY_MUST_BE_NON_EMPTY:
                # Python 3.11: LogRecord.taskName does not exist yet
                # (CPython gh-97263), so the formatter legitimately emits
                # null — tolerated instead of failing every entry (I-18).
                continue
            errors.append(f"empty required field: {key!r}")

    # level domain ---------------------------------------------------------
    level = obj.get("level")
    if level is not None and level not in VALID_LEVELS:
        errors.append(f"invalid level: {level!r}")

    # line must be an int --------------------------------------------------
    line_no = obj.get("line")
    if line_no is not None and not isinstance(line_no, int):
        errors.append(f"line is not an integer: {line_no!r}")

    # exception_tree (optional) -------------------------------------------
    et = obj.get("exception_tree")
    if et is not None:
        if not isinstance(et, dict):
            errors.append("exception_tree is not an object")
        else:
            for sub in ("class", "message", "notes"):
                if sub not in et:
                    errors.append(f"exception_tree missing field: {sub!r}")
            if "notes" in et and not isinstance(et["notes"], list):
                errors.append("exception_tree.notes is not a list")
            cause = et.get("cause")
            if cause is not None:
                if not isinstance(cause, dict):
                    errors.append("exception_tree.cause is not an object")
                else:
                    for sub in ("class", "message"):
                        if sub not in cause:
                            errors.append(f"exception_tree.cause missing field: {sub!r}")

    # stack_trace (optional) ----------------------------------------------
    st = obj.get("stack_trace")
    if st is not None:
        if not isinstance(st, str) or not st.strip():
            errors.append("stack_trace present but empty/non-string")

    return (len(errors) == 0, errors)


# ---------------------------------------------------------------------------
# Report building
# ---------------------------------------------------------------------------
def build_report(log_dir: Path) -> TelemetryReport:
    """Walk ``log_dir`` and produce a full ``TelemetryReport``."""
    report = TelemetryReport(log_dir=str(log_dir))

    for path, kind in find_log_files(log_dir):
        file_rep = FileReport(path=str(path), kind=kind)
        report.files.append(file_rep)

        try:
            for raw in iter_lines(path, kind):
                file_rep.total_lines += 1
                stripped = raw.strip()
                if not stripped:
                    continue

                # Every non-empty line counts as one attempted entry at the
                # report level, so JSON-parse failures are never lost from the
                # aggregate totals (they must be able to flip the verdict).
                report.total_entries += 1

                # JSON parse ------------------------------------------------
                try:
                    obj = json.loads(stripped)
                except json.JSONDecodeError as exc:
                    file_rep.invalid_entries += 1
                    report.invalid_entries += 1
                    file_rep.violations.append(
                        EntryViolation(str(path), file_rep.total_lines, [f"JSON parse error: {exc}"])
                    )
                    continue
                if not isinstance(obj, dict):
                    file_rep.invalid_entries += 1
                    report.invalid_entries += 1
                    file_rep.violations.append(
                        EntryViolation(str(path), file_rep.total_lines, ["top-level value is not an object"])
                    )
                    continue

                # Structural validation ------------------------------------
                valid, errors = validate_entry(obj)
                if not valid:
                    file_rep.invalid_entries += 1
                    report.invalid_entries += 1
                    file_rep.violations.append(
                        EntryViolation(str(path), file_rep.total_lines, errors)
                    )
                    # Still mine what forensic data we can from the entry.
                else:
                    file_rep.valid_entries += 1
                    report.valid_entries += 1

                level = str(obj.get("level", "UNKNOWN"))
                report.levels[level] = report.levels.get(level, 0) + 1

                if "asctime" in obj:
                    report.asctime_present += 1
                if "stack_trace" in obj:
                    report.stack_trace_present += 1
                if "exception_tree" in obj:
                    report.exception_tree_present += 1
                    et = obj["exception_tree"]
                    cls = et.get("class", "UNKNOWN") if isinstance(et, dict) else "UNKNOWN"
                    report.exception_classes[cls] = report.exception_classes.get(cls, 0) + 1

                provider = obj.get("provider")
                if provider:
                    report.providers_seen[str(provider)] = report.providers_seen.get(str(provider), 0) + 1

                for code in _extract_http_status_codes(obj):
                    report.http_status_codes[code] = report.http_status_codes.get(code, 0) + 1

                if _get_verb_seen(obj):
                    report.get_verb_seen += 1

        except (OSError, gzip.BadGzipFile) as exc:
            # Decompression / read failure: record and move on.
            file_rep.gzip_ok = False
            file_rep.gzip_error = f"{type(exc).__name__}: {exc}"

    return report


# ---------------------------------------------------------------------------
# Report rendering
# ---------------------------------------------------------------------------
def format_report(report: TelemetryReport) -> str:
    """Render a human-readable forensic report."""
    lines: list[str] = []
    bar = "=" * 72
    lines.append(bar)
    lines.append("  TRITON MONITOR — REPORTE FORENSE DE INTEGRIDAD DE TELEMETRÍA")
    lines.append(bar)
    lines.append(f"Directorio de logs: {report.log_dir}")
    lines.append(f"Archivos encontrados: {len(report.files)} "
                 f"({sum(1 for f in report.files if f.kind == 'active')} activo(s), "
                 f"{sum(1 for f in report.files if f.kind == 'gz')} comprimido(s))")
    lines.append("")

    # Per-file breakdown ---------------------------------------------------
    lines.append("-" * 72)
    lines.append("  Archivos")
    lines.append("-" * 72)
    if not report.files:
        lines.append("  (no se encontraron archivos de log)")
    for f in report.files:
        tag = "OK" if (f.gzip_ok and f.invalid_entries == 0) else "VIOLATIONS"
        lines.append(f"  • [{f.kind:6}] {f.path}")
        lines.append(f"      líneas={f.total_lines} válidas={f.valid_entries} "
                     f"inválidas={f.invalid_entries}  [{tag}]")
        if not f.gzip_ok:
            lines.append(f"      gzip/corruption error: {f.gzip_error}")
    lines.append("")

    # Aggregate totals -----------------------------------------------------
    lines.append("-" * 72)
    lines.append("  Totales")
    lines.append("-" * 72)
    lines.append(f"  Entradas parseadas: {report.total_entries}")
    lines.append(f"  Entradas válidas:   {report.valid_entries}")
    lines.append(f"  Entradas inválidas: {report.invalid_entries}")
    lines.append("")

    # Levels ---------------------------------------------------------------
    lines.append("-" * 72)
    lines.append("  Distribución por nivel")
    lines.append("-" * 72)
    if report.levels:
        for lvl in sorted(report.levels):
            lines.append(f"  {lvl:9}: {report.levels[lvl]}")
    else:
        lines.append("  (sin entradas)")
    lines.append("")

    # Exceptions -----------------------------------------------------------
    lines.append("-" * 72)
    lines.append("  Árbol de excepciones (exception_tree estructurado)")
    lines.append("-" * 72)
    lines.append(f"  Entradas con exception_tree: {report.exception_tree_present}")
    if report.exception_classes:
        for cls, count in sorted(report.exception_classes.items()):
            lines.append(f"    {cls}: {count}")
    else:
        lines.append("  (ninguna — una corrida caos debería emitirlas en las entradas ERROR del except*)")
    lines.append(f"  Entradas con stack_trace:    {report.stack_trace_present}")
    lines.append("")

    # HTTP forensic --------------------------------------------------------
    lines.append("-" * 72)
    lines.append("  Forense HTTP")
    lines.append("-" * 72)
    if report.http_status_codes:
        for code in sorted(report.http_status_codes):
            lines.append(f"  HTTP {code}: {report.http_status_codes[code]} aparición(es) verificada(s)")
    else:
        lines.append("  (no se detectaron códigos de estado HTTP)")
    lines.append(f"  Verbo GET detectado en {report.get_verb_seen} entrada(s) (contador informativo)")
    lines.append("")

    # Metadata integrity ---------------------------------------------------
    lines.append("-" * 72)
    lines.append("  Integridad de metadatos")
    lines.append("-" * 72)
    lines.append(f"  Campos requeridos: {', '.join(REQUIRED_FIELDS)}")
    lines.append(f"  Entradas con 'asctime' (legado pre-PR #5 — el formateador actual ya no lo emite): "
                 f"{report.asctime_present}")
    if report.providers_seen:
        lines.append(f"  Proveedores vistos: "
                     + ", ".join(f"{p}×{n}" for p, n in sorted(report.providers_seen.items())))
    lines.append("")

    # Gzip -----------------------------------------------------------------
    lines.append("-" * 72)
    lines.append("  Descompresión gzip")
    lines.append("-" * 72)
    gz_files = [f for f in report.files if f.kind == "gz"]
    if gz_files:
        for f in gz_files:
            status = "OK" if f.gzip_ok else f"FALLO — {f.gzip_error}"
            lines.append(f"  • {f.path}: {status}")
    else:
        lines.append("  (no hay archivos .gz rotados; nada que descomprimir)")
    lines.append("")

    # Violations -----------------------------------------------------------
    lines.append("-" * 72)
    lines.append("  Violaciones de integridad")
    lines.append("-" * 72)
    all_violations = [v for f in report.files for v in f.violations]
    if all_violations:
        for v in all_violations:
            lines.append(f"  ✗ {Path(v.path).name}:{v.line_no} → {'; '.join(v.errors)}")
    elif not report.files:
        lines.append("  ✗ No se encontraron archivos de log para validar.")
    else:
        lines.append("  ✓ Sin violaciones. Toda entrada es JSON válido con los campos requeridos.")
    lines.append("")

    # Verdict --------------------------------------------------------------
    lines.append(bar)
    if not report.files:
        verdict = "FALLO — no se encontraron logs"
    elif report.has_violations:
        verdict = "FALLO — se detectaron violaciones"
    else:
        verdict = "OK — integridad verificada"
    lines.append(f"  Veredicto: {verdict}")
    lines.append(bar)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Bundled self-test (validates the documented exception_tree shape)
# ---------------------------------------------------------------------------
# Las causas y stack_traces sintéticos siguen el formato ACTUAL de httpx 0.28
# (issue I-20): los mensajes de estatus ya no terminan con el verbo, así que
# la evidencia del GET vive en el frame del call-site del traceback
# (``await client.get(...)``) — igual que en los logs reales. Las tres
# entradas cubren las clases semánticas completas del dominio:
#
# * Azure 504 (estatus HTTP fallido) y GCP /xml (payload corrupto) mapean a
#   CorruptedPayloadError según §2.2.2 de la consigna: los estatus fallidos
#   son "respuestas corruptas o estatus fallidos HTTP" y el rol prescribe
#   ``raise CorruptedPayloadError(...) from error_nativo`` para
#   ``httpx.HTTPStatusError``.
# * NetworkPeeringError NO fue eliminada: sigue siendo la clase de
#   DNS/ruteo/transporte según §2.2.1 y §2.2.5c ("fallos catastróficos de
#   DNS"); la muestra AWS la ejercita con causa nativa httpx.ConnectError —
#   lo mismo que produce la suite de caos DNS (test 9) contra hosts
#   ``*.invalid`` y las corridas offline.
#
# El validador en sí (``validate_entry``) es puramente estructural y nunca
# hardcodea clases de excepción: las muestras demuestran explícitamente las
# tres clases que el pipeline emite.
_SELF_TEST_SAMPLES = (
    # 1) Estatus HTTP fallido (Azure 504): mapea a CorruptedPayloadError
    #    según §2.2.2 ("raise CorruptedPayloadError(...) from error_nativo").
    '{"timestamp": "2026-08-25T22:49:05Z", "level": "ERROR", "logger": "triton_monitor", '
    '"message": "Fallo: El proveedor Azure respondió con un error HTTP: 504.", '
    '"async_task": "TritonTask-Azure", "thread_name": "MainThread", '
    '"filename": "core.py", "line": 81, "provider": "Azure", '
    '"exception_tree": {"class": "CorruptedPayloadError", '
    '"message": "El proveedor Azure respondió con un error HTTP: 504.", '
    '"notes": ["Provider_ID: Azure", "HTTP_Status_Code: 504"], '
    '"cause": {"class": "HTTPStatusError", '
    '"message": "Server error \'504 GATEWAY TIMEOUT\' for url \'https://httpbin.org/status/504\'", '
    '"notes": []}}, '
    '"stack_trace": "Traceback (most recent call last):\\n'
    '  File \\"src/triton_telemetry/core.py\\", line 72, in _execute_telemetry_exchange\\n'
    '    response = await client.get(url, timeout=timeout)"}',
    # 2) Payload corrupto (GCP /xml): HTTP 200 + cuerpo XML → JSONDecodeError,
    #    también CorruptedPayloadError (§2.2.2), con notas de contexto de
    #    Endpoint/Content-Type (Escenario C de la consigna).
    '{"timestamp": "2026-08-25T22:49:06Z", "level": "ERROR", "logger": "triton_monitor", '
    '"message": "Fallo: El proveedor GCP devolvió un payload no serializable o con errores de paridad.", '
    '"async_task": "TritonTask-GCP", "thread_name": "MainThread", '
    '"filename": "core.py", "line": 78, "provider": "GCP", '
    '"exception_tree": {"class": "CorruptedPayloadError", '
    '"message": "El proveedor GCP devolvió un payload no serializable o con errores de paridad.", '
    '"notes": ["Provider_ID: GCP | Endpoint: https://httpbin.org/xml | Content-Type: application/xml"], '
    '"cause": {"class": "JSONDecodeError", '
    '"message": "Expecting value: line 1 column 1 (char 0)", '
    '"notes": []}}, '
    '"stack_trace": "Traceback (most recent call last):\\n'
    '  File \\"src/triton_telemetry/core.py\\", line 78, in _execute_telemetry_exchange\\n'
    '    payload = response.json()"}',
    # 3) Fallo físico de transporte/DNS (AWS): NetworkPeeringError según
    #    §2.2.1/§2.2.5c ("fallos catastróficos de DNS"), causa nativa
    #    httpx.ConnectError — lo que produce la suite DNS (test 9) contra
    #    hosts *.invalid y las corridas offline.
    '{"timestamp": "2026-08-25T22:49:07Z", "level": "ERROR", "logger": "triton_monitor", '
    '"message": "Fallo: Fallo físico de transporte o ruteo al intentar alcanzar AWS.", '
    '"async_task": "TritonTask-AWS", "thread_name": "MainThread", '
    '"filename": "core.py", "line": 72, "provider": "AWS", '
    '"exception_tree": {"class": "NetworkPeeringError", '
    '"message": "Fallo físico de transporte o ruteo al intentar alcanzar AWS.", '
    '"notes": ["Provider_ID: AWS | Native_Error_Type: ConnectError"], '
    '"cause": {"class": "ConnectError", '
    '"message": "[Errno -2] Name or service not known", '
    '"notes": []}}, '
    '"stack_trace": "Traceback (most recent call last):\\n'
    '  File \\"src/triton_telemetry/core.py\\", line 72, in _execute_telemetry_exchange\\n'
    '    response = await client.get(url, timeout=timeout)"}',
)


def _self_test() -> int:
    """Valida las tres entradas sintéticas de las clases semánticas del dominio.

    Cada entrada ejercita el camino completo de ``exception_tree`` (clase,
    mensaje, notas y causa encadenada) más los contadores forenses HTTP. El
    verbo GET se verifica solo en la muestra de estatus: la evidencia vive en
    el frame del call-site del traceback (issue I-20).
    """
    # (clase esperada del árbol, clase esperada de la causa, código HTTP
    #  esperado o None).
    expectations = (
        ("CorruptedPayloadError", "HTTPStatusError", 504),
        ("CorruptedPayloadError", "JSONDecodeError", None),
        ("NetworkPeeringError", "ConnectError", None),
    )

    print("Self-test samples:")
    ok = True

    for sample, (tree_class, cause_class, expected_code) in zip(
        _SELF_TEST_SAMPLES, expectations
    ):
        obj = json.loads(sample)
        valid, errors = validate_entry(obj)
        codes = _extract_http_status_codes(obj)
        tree = obj.get("exception_tree") or {}
        cause = tree.get("cause") or {}

        entry_ok = valid and tree.get("class") == tree_class
        entry_ok = entry_ok and cause.get("class") == cause_class
        if expected_code is not None:
            entry_ok = (
                entry_ok
                and expected_code in codes
                and _get_verb_seen(obj)
            )
        ok = ok and entry_ok

        print(
            f"  [{tree.get('class')} <- causa {cause.get('class')}] "
            f"valid={valid} errors={errors}"
        )
        print(
            f"    http_status_codes={codes} "
            f"get_verb_seen={_get_verb_seen(obj)} -> "
            f"{'OK' if entry_ok else 'FALLO'}"
        )

    print(f"  Veredicto: {'OK' if ok else 'FALLO'}")
    return 0 if ok else 1


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="validate_telemetry",
        description="Forensic validator for Triton Monitor JSON telemetry logs.",
    )
    parser.add_argument(
        "log_dir",
        nargs="?",
        default=None,
        help="Directory containing triton_services.log (default: project root).",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Validate a bundled synthetic exception entry and exit.",
    )
    args = parser.parse_args(argv)

    if args.self_test:
        return _self_test()

    if args.log_dir:
        log_dir = Path(args.log_dir).resolve()
    else:
        # Default to the project root (parent of tests/).
        log_dir = Path(__file__).resolve().parent.parent

    report = build_report(log_dir)
    print(format_report(report))
    if not report.files:
        return 1
    return 1 if report.has_violations else 0


if __name__ == "__main__":
    sys.exit(main())
