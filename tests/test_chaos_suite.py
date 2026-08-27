"""Sub-punto 1 — Suite de Simulacion de Caos for the Triton Monitor CLI.

Every test exercises the CLI as a black box through ``subprocess``. We never
import from ``src/`` so the production package is exercised exactly the way a
real operator invokes it, and other branches remain untouched.

Markers
-------
``@pytest.mark.integration`` — needs live network (jsonplaceholder / httpbin).
``@pytest.mark.unit``        — pure argparse validation, no network required.

Known issue accounted for
-------------------------
``asyncio.TaskGroup`` in ``core.py`` is fail-fast: the first provider task to
raise cancels its siblings, so in chaos mode only ONE ``except*`` block fires
per run instead of three concurrent ones. The chaos assertions therefore check
that *at least one* forensic marker appears, not all three. This race is
tracked for another integrante and is intentionally not fixed here.
"""
from __future__ import annotations

import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

# Forensic markers emitted by the ``except*`` blocks in ``app_operator.py``.
# ``CONEXION`` carries an accent in the real output ("CONEXIÓN"); we also
# accept ``ROUTING`` which always co-occurs, so encoding hiccups never flake.
TIMEOUT_MARKER = "TIMEOUTS"
CORRUPT_MARKER = "CORRUPTOS"
CONNECTION_MARKERS = ("CONEXIÓN", "ROUTING")
ALL_CHAOS_MARKERS = (TIMEOUT_MARKER, CORRUPT_MARKER, *CONNECTION_MARKERS)

VALID_CLUSTER = "cluster-us-east-01"
CLI_TIMEOUT = 30  # seconds per subprocess


# ===========================================================================
# 1. Timeout chaos
# ===========================================================================
@pytest.mark.integration
def test_timeout_chaos_forces_provider_timeout(run_cli, requires_network):
    """Chaos + 0.1s timeout must surface a ProviderTimeoutError.

    In chaos mode AWS hits ``httpbin.org/delay/3`` which sleeps 3s; with a
    0.1s timeout the request times out well before the network round-trip of
    the other providers, so ProviderTimeoutError wins the TaskGroup race and
    the ``except* ProviderTimeoutError`` block prints the TIMEOUTS marker.
    Using ``--chaos`` (rather than a bare 0.1s against jsonplaceholder) keeps
    the trigger deterministic regardless of how fast the local link is.
    """
    result = run_cli("AWS", "Azure", "GCP", "-c", VALID_CLUSTER, "--chaos", "-t", "0.1")

    assert result.returncode == 0, f"expected exit 0 (except* caught it), got {result.returncode}\nstderr:\n{result.stderr}"
    assert TIMEOUT_MARKER in result.stdout, (
        f"expected '{TIMEOUT_MARKER}' in stdout\nstdout:\n{result.stdout}"
    )


# ===========================================================================
# 2. Chaos mode triggers at least one exception group handler
# ===========================================================================
@pytest.mark.integration
def test_chaos_mode_triggers_exceptions(run_cli, requires_network):
    """``--chaos -t 1.5`` must fire at least one ``except*`` block.

    Azure's ``httpbin.org/status/504`` returns immediately with HTTP 504 and
    typically wins the TaskGroup race, surfacing the CONEXIÓN/ROUTING marker.
    Because of the fail-fast race we only assert that *some* forensic marker
    appears — not all three. Exit code is 0 because ``except*`` contains it.
    """
    result = run_cli("AWS", "Azure", "GCP", "-c", VALID_CLUSTER, "--chaos", "-t", "1.5")

    assert result.returncode == 0, f"expected exit 0, got {result.returncode}\nstderr:\n{result.stderr}"
    combined = result.stdout + result.stderr
    assert any(marker in combined for marker in ALL_CHAOS_MARKERS), (
        f"expected at least one of {ALL_CHAOS_MARKERS} in output\nstdout:\n{result.stdout}"
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
