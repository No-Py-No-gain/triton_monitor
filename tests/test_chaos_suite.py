"""Sub-punto 1 — Suite de Simulacion de Caos for the Triton Monitor CLI.

Almost every test exercises the CLI as a black box through ``subprocess`` so
the production package runs exactly the way a real operator invokes it. The
single sanctioned exception is the DNS-collapse test (issue I-11): the CLI
exposes no URL parameter, so that test imports ``triton_telemetry`` directly
to point ``scan_all_providers`` at a reserved ``*.invalid`` host — see the
justification in the test itself. No ``src/`` file is ever modified.

Markers
-------
``@pytest.mark.integration`` — exercises the real network stack or the real
CLI/logging pipeline (jsonplaceholder / httpbin / DNS resolution).
``@pytest.mark.unit``        — pure validation logic, no network required.

Concurrent incident coverage (post-fix reality)
-----------------------------------------------
``asyncio.TaskGroup``'s native fail-fast race (the first provider task to
raise cancels its siblings, so only one ``except*`` block would fire) is
SOLVED in ``core.py`` by the ``_run_mission_with_capture`` wrapper
(``core.py:220-240``): every mission is allowed to finish and its incident
returns as a sentinel value, so the re-raised ExceptionGroup is complete and
every ``except*`` block whose class is present fires. A chaos run therefore
surfaces ALL the concurrent incidents the current triggers produce —
empirically three: the AWS timeout (ProviderTimeoutError) plus the Azure 504
and GCP ``/xml`` pair (both CorruptedPayloadError). The GCP trigger is the
plantilla's ``httpbin.org/xml`` (HTTP 200 with an XML body → ``response.json()``
raises), and failed HTTP statuses / corrupt payloads both map to
CorruptedPayloadError per the consigna's role texts (§2.2.1: "respuestas
corruptas o estatus fallidos HTTP"; §2.2.2 prescribes ``raise
CorruptedPayloadError(...) from error_nativo``). NetworkPeeringError remains
DNS/routing-only and is exercised in test 9 via reserved ``*.invalid`` hosts.
"""
from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from tests import validate_telemetry

# Forensic markers emitted by the ``except*`` blocks in ``app_operator.py``.
# ``CORRUPT_MARKER`` matches the corrupt/status header that groups the Azure
# 504 and GCP /xml incidents (both CorruptedPayloadError under the consigna
# role-text mapping §2.2.1/§2.2.2). A chaos run no longer produces the
# DNS/CONEXIÓN header: NetworkPeeringError coverage lives in test 9, which
# asserts the exception classes in-process against ``*.invalid`` hosts.
TIMEOUT_MARKER = "TIMEOUTS"
CORRUPT_MARKER = "CORRUPTOS O ESTATUS HTTP FALLIDOS"

VALID_CLUSTER = "cluster-us-east-01"
CLI_TIMEOUT = 30  # seconds per subprocess


# ===========================================================================
# 1. Timeout chaos
# ===========================================================================
@pytest.mark.integration
def test_timeout_chaos_forces_provider_timeout(run_cli, requires_network):
    """Chaos + 0.1s timeout must surface a ProviderTimeoutError.

    In chaos mode AWS hits ``httpbin.org/delay/3`` which sleeps 3s; with a
    0.1s timeout the AWS request times out long before the endpoint answers,
    so the ``except* ProviderTimeoutError`` block prints the TIMEOUTS marker
    (the sibling missions still complete thanks to the anti-fail-fast
    wrapper — see the module docstring). Using ``--chaos`` (rather than a
    bare 0.1s against jsonplaceholder) keeps the trigger deterministic
    regardless of how fast the local link is.
    """
    result = run_cli("AWS", "Azure", "GCP", "-c", VALID_CLUSTER, "--chaos", "-t", "0.1")

    assert result.returncode == 0, f"expected exit 0 (except* caught it), got {result.returncode}\nstderr:\n{result.stderr}"
    assert TIMEOUT_MARKER in result.stdout, (
        f"expected '{TIMEOUT_MARKER}' in stdout\nstdout:\n{result.stdout}"
    )


# ===========================================================================
# 2. Chaos mode surfaces the COMPLETE concurrent incident set
# ===========================================================================
@pytest.mark.integration
def test_chaos_mode_triggers_exceptions(run_cli, requires_network):
    """``--chaos -t 1.5`` must surface every concurrent ``except*`` block.

    The anti-fail-fast wrapper (``_run_mission_with_capture``) lets all three
    missions finish, so the ExceptionGroup re-raised to ``asyncio.run`` is
    complete and every ``except*`` class present in it fires: the AWS timeout
    (ProviderTimeoutError -> TIMEOUTS header) and the Azure 504 + GCP ``/xml``
    pair (both CorruptedPayloadError -> the corrupt/status header reporting
    "2 incidentes").

    The GCP trigger is the plantilla's restored ``httpbin.org/xml`` endpoint:
    HTTP 200 with an XML body, so ``response.json()`` raises a genuine
    ``json.JSONDecodeError``. Both incident flavors map to
    CorruptedPayloadError per the consigna's role texts — §2.2.1 defines it as
    covering "respuestas corruptas o estatus fallidos HTTP" and §2.2.2
    prescribes ``raise CorruptedPayloadError(...) from error_nativo`` for
    ``httpx.HTTPStatusError`` — while NetworkPeeringError stays
    DNS/routing-only (exercised in test 9). We assert every marker the
    triggers produce plus the forensic notes proving both flavors reached the
    group — direct evidence for the consigna's "fallos simultáneos agrupados
    y capturados quirúrgicamente". Exit code is 0 because ``except*``
    contains it.
    """
    result = run_cli("AWS", "Azure", "GCP", "-c", VALID_CLUSTER, "--chaos", "-t", "1.5")

    assert result.returncode == 0, f"expected exit 0, got {result.returncode}\nstderr:\n{result.stderr}"
    combined = result.stdout + result.stderr

    # Timeout incident: AWS /delay/3 cannot answer within 1.5s.
    assert TIMEOUT_MARKER in combined, (
        f"expected '{TIMEOUT_MARKER}' in output\nstdout:\n{result.stdout}"
    )
    # Corrupt/status incidents: Azure 504 + GCP /xml grouped in one header.
    assert CORRUPT_MARKER in combined, (
        f"expected '{CORRUPT_MARKER}' in output\nstdout:\n{result.stdout}"
    )
    # Forensic note: the Azure 504 status reached the group — now under the
    # corrupt/status header per the §2.2.1/§2.2.2 role-text mapping.
    assert "HTTP_Status_Code: 504" in combined, (
        "expected the Azure 504 forensic note in output\nstdout:\n" + result.stdout
    )
    # Forensic note: the GCP /xml corruption reached the group carrying its
    # own Provider_ID/Endpoint/Content-Type context (the notes added together
    # with the restored trigger so the incident produces FORENSE output).
    assert "Provider_ID: GCP" in combined, (
        "expected the GCP forensic note in output\nstdout:\n" + result.stdout
    )


# ===========================================================================
# 3. Verbose surfaces DEBUG records on stdout
# ===========================================================================
@pytest.mark.integration
def test_verbose_shows_debug(run_cli, requires_network):
    """``--verbose`` lowers the stdout handler to DEBUG so request traces show.

    ``core.py`` logs ``logger.debug("Petición asíncrona iniciada hacia ...")``
    per provider; with the console handler at DEBUG those lines render as
    ``[DEBUG]`` on stdout. Nominal mode is used so the run succeeds quickly.
    """
    result = run_cli("AWS", "Azure", "GCP", "-c", VALID_CLUSTER, "-t", "3.0", "--verbose")

    assert result.returncode == 0, f"expected exit 0, got {result.returncode}\nstderr:\n{result.stderr}"
    assert "[DEBUG]" in result.stdout, (
        f"expected '[DEBUG]' in stdout under --verbose\nstdout:\n{result.stdout}"
    )


# ===========================================================================
# 4. Quiet suppresses INFO records on stdout
# ===========================================================================
@pytest.mark.integration
def test_quiet_suppresses_info(run_cli, requires_network):
    """``--quiet`` raises the stdout handler to WARNING, hiding INFO lines.

    In nominal mode there are no WARNING/ERROR records, so stdout should be
    empty of ``[INFO]``. We assert absence of ``[INFO]`` on stdout only
    (stderr may still carry argparse usage on error, which is out of scope).
    """
    result = run_cli("AWS", "-c", VALID_CLUSTER, "-t", "3.0", "--quiet")

    assert result.returncode == 0, f"expected exit 0, got {result.returncode}\nstderr:\n{result.stderr}"
    assert "[INFO]" not in result.stdout, (
        f"expected no '[INFO]' on stdout under --quiet\nstdout:\n{result.stdout}"
    )


# ===========================================================================
# 5. --verbose and --quiet are mutually exclusive (argparse, exit 2)
# ===========================================================================
@pytest.mark.unit
def test_verbose_quiet_mutually_exclusive(run_cli):
    """argparse rejects ``--verbose --quiet`` with exit code 2.

    No network is touched: argparse validates the mutually exclusive group
    before ``async_main`` reaches ``scan_all_providers``.
    """
    result = run_cli("--verbose", "--quiet", "AWS", "-c", VALID_CLUSTER)

    assert result.returncode == 2, f"expected exit 2, got {result.returncode}"
    assert "not allowed with argument" in result.stderr, (
        f"expected 'not allowed with argument' in stderr\nstderr:\n{result.stderr}"
    )


# ===========================================================================
# 6. Invalid cluster id rejected by the sanitizer (exit 2)
# ===========================================================================
@pytest.mark.unit
def test_invalid_cluster_id_rejected(run_cli):
    """``parse_cluster_id`` rejects ids that break ``cluster-<region>-<word>-<NN>``.

    ``invalid-id`` lacks the ``cluster-`` prefix and the two-digit suffix, so
    argparse raises ``ArgumentTypeError`` and exits with code 2.
    """
    result = run_cli("AWS", "-c", "invalid-id")

    assert result.returncode == 2, f"expected exit 2, got {result.returncode}"


# ===========================================================================
# 7. Out-of-range timeout rejected by the sanitizer (exit 2)
# ===========================================================================
@pytest.mark.unit
def test_invalid_timeout_rejected(run_cli):
    """``parse_timeout`` rejects values outside ``[0.1, 5.0]``.

    ``9.0`` exceeds the upper bound so argparse exits with code 2.
    """
    result = run_cli("AWS", "-c", VALID_CLUSTER, "-t", "9.0")

    assert result.returncode == 2, f"expected exit 2, got {result.returncode}"


# ===========================================================================
# 8. Concurrent massive CLI invocations (the "masivas" part)
# ===========================================================================
# A mix of nominal runs (exit 0) and argparse-error runs (exit 2, no network).
# Error cases are included so the "all exit codes in {0,2}" assertion covers
# both branches without depending on httpbin flakiness.
_CONCURRENT_JOBS = [
    ("AWS", "Azure", "GCP", "-c", VALID_CLUSTER, "-t", "3.0"),
    ("AWS", "GCP", "-c", VALID_CLUSTER, "-t", "2.0"),
    ("Azure", "-c", VALID_CLUSTER, "-t", "3.0", "--verbose"),
    ("GCP", "AWS", "-c", VALID_CLUSTER, "-t", "2.5"),
    ("AWS", "-c", VALID_CLUSTER, "-t", "9.0"),          # invalid timeout -> exit 2
    ("AWS", "-c", "bad-id"),                             # invalid cluster -> exit 2
]


@pytest.mark.integration
def test_concurrent_massive_cli_invocations(cli_path, requires_network):
    """Five-plus CLI invocations fired in parallel must not hang or crash.

    Each invocation runs in its own temp working directory so the per-process
    ``triton_services.log`` never contends on a shared file handle — this
    isolates true concurrency from log-rotation races. We only assert that
    every subprocess returns (no ``TimeoutExpired``) and that every exit code
    is in ``{0, 2}`` (success or argparse validation error), with at least one
    real success proving the batch did actual work.
    """
    results: list[tuple[int, int]] = []  # (index, returncode)

    def _invoke(idx: int, args: tuple[str, ...]) -> tuple[int, int]:
        with tempfile.TemporaryDirectory(prefix=f"triton_conc_{idx}_") as tmpcwd:
            proc = subprocess.run(
                [sys.executable, str(cli_path), *args],
                capture_output=True,
                text=True,
                timeout=CLI_TIMEOUT,
                cwd=tmpcwd,
            )
            return idx, proc.returncode

    with ThreadPoolExecutor(max_workers=len(_CONCURRENT_JOBS)) as pool:
        futures = [pool.submit(_invoke, i, args) for i, args in enumerate(_CONCURRENT_JOBS)]
        for fut in as_completed(futures):
            results.append(fut.result())  # raises TimeoutExpired -> test fails loudly

    assert len(results) == len(_CONCURRENT_JOBS), "some concurrent invocations did not complete"

    codes = [code for _, code in results]
    bad = [code for code in codes if code not in (0, 2)]
    assert not bad, f"unexpected exit codes (crashes): {bad}"

    assert 0 in codes, "expected at least one successful (exit 0) invocation in the batch"


# ===========================================================================
# 9. DNS/URL collapse: nonexistent host -> NetworkPeeringError (I-11)
# ===========================================================================
@pytest.mark.integration
def test_dns_collapse_maps_to_network_peering_error(cli_path, monkeypatch):
    """A base URL on a reserved ``*.invalid`` host must collapse into
    ``NetworkPeeringError`` via ``httpx.RequestError`` (consigna 2.2.6.1:
    "modificando la base URL a hosts inexistentes para gatillar
    NetworkPeeringError"; issue I-11, option i).

    This is the ONE test that imports ``triton_telemetry`` instead of driving
    the CLI as a subprocess: the CLI exposes no URL parameter, so a black-box
    run cannot inject a nonexistent host, and the env-var override variant
    (option ii) would touch ``core.py`` — another lane. Importing the package
    without modifying it is the sanctioned compromise. ``.invalid`` is
    reserved by RFC 6761 and never resolves, so the DNS attempt fails both
    online (NXDOMAIN) and offline; the test needs the resolution to FAIL,
    which is why it does not use the ``requires_network`` probe. Marked
    integration because it still exercises the real resolver/socket stack.
    """
    monkeypatch.syspath_prepend(str(cli_path.parent))  # <repo>/src on sys.path
    from triton_telemetry import core
    import httpx

    # Point every nominal mission at a host that cannot exist. The missions
    # read this registry at call time, so the runtime patch takes effect
    # without touching any src/ file.
    monkeypatch.setattr(
        core,
        "PROVIDER_ENDPOINTS",
        {p: f"https://triton-dns-collapse.invalid/posts/{i}"
         for i, p in enumerate(("AWS", "Azure", "GCP"), start=1)},
    )

    with pytest.raises(ExceptionGroup) as excinfo:
        asyncio.run(core.scan_all_providers(["AWS", "Azure", "GCP"], 5.0))

    group = excinfo.value
    assert len(group.exceptions) == 3, (
        "expected all three concurrent DNS incidents grouped, got "
        f"{[type(e).__name__ for e in group.exceptions]}"
    )
    for exc in group.exceptions:
        assert isinstance(exc, core.NetworkPeeringError), (
            f"expected NetworkPeeringError, got {type(exc).__name__}"
        )
        # The exact path under test (H2): the native transport error (DNS /
        # connect failure) must be the recorded cause of the semantic error.
        assert isinstance(exc.__cause__, httpx.RequestError), (
            f"expected an httpx.RequestError cause, got {type(exc.__cause__).__name__}"
        )
        notes = getattr(exc, "__notes__", [])
        assert any("Native_Error_Type:" in note for note in notes), (
            f"expected a 'Native_Error_Type' forensic note, got {notes}"
        )


# ===========================================================================
# 10. Chaos run persists the exception tree to the JSON log (I-13)
# ===========================================================================
@pytest.mark.integration
def test_chaos_run_logs_exception_tree(run_cli, log_file):
    """A chaos run must persist ``exception_tree`` entries in the JSON log.

    This test consumes the ``log_file`` fixture (issue I-13: it previously
    had zero consumers — consuming it was preferred over retiring it because
    it delivers the end-to-end evidence the fixture was designed for:
    ``exc_info=exc`` passed by ``app_operator.py`` survives
    ``PreservingQueueHandler`` and lands in the JSON as a recursive
    ``exception_tree``). The CLI runs with ``cwd=log_file.parent`` so the
    per-process log lands exactly on the fixture path regardless of where
    pytest was invoked from. ``requires_network`` is deliberately NOT used:
    the assertions hold whether the providers answer or not (unreachable
    providers surface as NetworkPeeringError trees — the DNS-collapse case).
    Validation reuses the pure functions of ``tests/validate_telemetry.py``
    instead of re-implementing the checks.
    """
    result = run_cli(
        "AWS", "Azure", "GCP", "-c", VALID_CLUSTER, "--chaos", "-t", "1.5",
        cwd=log_file.parent,
    )

    assert result.returncode == 0, f"expected exit 0, got {result.returncode}\nstderr:\n{result.stderr}"
    assert log_file.is_file(), f"expected the CLI to write {log_file}"

    entries: list[dict] = []
    for line in log_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entries.append(json.loads(line))
        except json.JSONDecodeError:
            continue  # stale/historical line, not this run's concern

    error_entries = [e for e in entries if e.get("level") == "ERROR"]
    assert error_entries, "chaos run must log ERROR entries"

    tree_entries = [e for e in error_entries if isinstance(e.get("exception_tree"), dict)]
    assert tree_entries, (
        "expected ERROR entries carrying exception_tree "
        "(exc_info must survive the logging queue)"
    )
    for entry in tree_entries:
        valid, errors = validate_telemetry.validate_entry(entry)
        assert valid, f"validator rejected a logged exception entry: {errors}"
        # exception_tree entries can only exist post-PR #5 (the queue now
        # preserves exc_info), and post-PR #5 never emits asctime — so a
        # leak here would be a genuine regression (issue I-12).
        assert "asctime" not in entry, "asctime leaked into an exception entry"

    classes = {e["exception_tree"].get("class") for e in tree_entries}
    # Online, a chaos run surfaces ProviderTimeoutError (AWS /delay/3) plus
    # CorruptedPayloadError (Azure 504 + GCP /xml per §2.2.1/§2.2.2); offline
    # (httpbin unreachable) every mission collapses into NetworkPeeringError
    # via httpx.RequestError — the DNS path. The intersection keeps the
    # assertion valid in both worlds.
    assert classes & {"ProviderTimeoutError", "CorruptedPayloadError", "NetworkPeeringError"}, (
        f"expected chaos-triggered exception classes in the tree, got {classes}"
    )


# ===========================================================================
# 11. Validator tolerates a null task key below Python 3.12 (I-18, unit)
# ===========================================================================
@pytest.mark.unit
def test_validator_tolerates_null_task_key_below_python_312(monkeypatch):
    """``LogRecord.taskName`` exists only from Python 3.12 (CPython gh-97263),
    so on 3.11 — the documented minimum — the formatter emits
    ``task_name``/``async_task`` as null. The validator must tolerate the
    EMPTY task key there instead of failing every entry (issue I-18, option
    a). The 3.11 behavior is simulated by forcing the validator's version
    flag off; the suite itself runs on 3.14, where the non-empty requirement
    stays active and is asserted first. Absence of the key is never
    tolerated on any interpreter.
    """
    entry = {key: "placeholder" for key in validate_telemetry.REQUIRED_FIELDS}
    entry.update(
        {
            "timestamp": "2026-09-01T00:00:00Z",
            "level": "ERROR",
            "logger": "triton_monitor",
            "message": "Fallo de conexión",
            "thread_name": "MainThread",
            "filename": "core.py",
            "line": 81,
            "async_task": None,  # what Python 3.11 emits (no taskName attr)
        }
    )

    # On >= 3.12 (this suite's interpreter) the empty task key is a violation.
    valid, errors = validate_telemetry.validate_entry(entry)
    if sys.version_info >= (3, 12):
        assert not valid
        assert "empty required field: 'async_task'" in errors

    # Simulated Python 3.11: the null task key is tolerated.
    monkeypatch.setattr(validate_telemetry, "_TASK_KEY_MUST_BE_NON_EMPTY", False)
    valid, errors = validate_telemetry.validate_entry(entry)
    assert valid, f"entry with null async_task must be valid on 3.11: {errors}"

    # A MISSING task key is still a violation on every interpreter.
    del entry["async_task"]
    valid, errors = validate_telemetry.validate_entry(entry)
    assert not valid
    assert "missing required field: 'async_task'" in errors
