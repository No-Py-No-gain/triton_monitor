"""Pytest configuration and shared fixtures for the Triton Monitor chaos suite.

This file is intentionally side-effect free with respect to the ``src/`` tree:
the chaos suite only ever talks to the CLI through ``subprocess`` so the
production package is never imported in-process during the tests.
"""
from __future__ import annotations

import os
import socket
import subprocess
import sys
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Path constants
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CLI_PATH = PROJECT_ROOT / "src" / "app_operator.py"
LOG_FILE = PROJECT_ROOT / "triton_services.log"

# Hosts used by the CLI. ``jsonplaceholder`` feeds nominal mode, ``httpbin``
# feeds chaos mode. We probe the placeholder first because every integration
# test (even the chaos ones) tolerates a httpbin outage via NetworkPeeringError.
_NETWORK_PROBES = [("jsonplaceholder.typicode.com", 443), ("httpbin.org", 443)]


# ---------------------------------------------------------------------------
# Marker registration (avoids pytest "unknown mark" warnings)
# ---------------------------------------------------------------------------
def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers", "integration: tests that require live network connectivity"
    )
    config.addinivalue_line(
        "markers", "unit: tests that validate argparse behaviour without network"
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def cli_path() -> Path:
    """Absolute path to the CLI entry point ``src/app_operator.py``."""
    return CLI_PATH


@pytest.fixture
def run_cli(cli_path):
    """Return a factory that invokes the CLI as a subprocess.

    Usage::

        def test_something(run_cli):
            result = run_cli("AWS", "-c", "cluster-us-east-01", "-t", "3.0")
            assert result.returncode == 0

    The factory mirrors the contract requested by the enunciado: it shells out
    to ``sys.executable`` with ``capture_output=True, text=True`` and a 30s
    guard. An optional ``cwd`` lets callers isolate the per-process log file
    (used by the concurrency test to avoid shared-file contention).
    """

    def _run(*args: str, cwd: str | os.PathLike[str] | None = None, timeout: int = 30):
        cmd = [sys.executable, str(cli_path), *args]
        return subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=str(cwd) if cwd is not None else None,
        )

    return _run


@pytest.fixture
def requires_network():
    """Skip the test when no outbound connectivity is available.

    Performs a short TCP connect to the providers the CLI depends on. If
    neither answers we skip rather than emit a false failure.
    """
    for host, port in _NETWORK_PROBES:
        try:
            with socket.create_connection((host, port), timeout=5):
                return  # At least one probe is reachable.
        except OSError:
            continue
    pytest.skip("No network connectivity to jsonplaceholder/httpbin; skipping integration test.")


@pytest.fixture
def log_file() -> Path:
    """Path to the active (uncompressed) log file at the project root."""
    return LOG_FILE
