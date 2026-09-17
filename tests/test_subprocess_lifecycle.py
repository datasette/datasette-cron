"""Subprocess end-to-end tests for the new datasette lifecycle port.

These spawn a real `datasette serve` process (shaped after core's
tests/test_cli_serve_server.py) to exercise the actual ASGI lifespan --
uvicorn binding a port, the `lifespan.startup`/`lifespan.shutdown` events,
real signal delivery -- rather than the pytest_asyncio.fixture shortcuts
(`invoke_startup()` / `start_background_tasks()`) used elsewhere in this
suite. That's the only way to actually prove the asgi_wrapper workaround
this port deletes is gone: the pytest fixtures never exercised the
first-request code path in the first place.
"""

import signal
import socket
import sqlite3
import subprocess
import sys
import time


def _find_free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _poll_until(predicate, timeout, proc, on_timeout_msg, interval=0.1):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            raise AssertionError(
                f"datasette serve exited early (code {proc.returncode}):\n"
                + proc.stdout.read().decode("utf-8")
            )
        if predicate():
            return
        time.sleep(interval)
    raise AssertionError(on_timeout_msg)


def _terminate(proc):
    if proc.poll() is None:
        proc.kill()
        proc.wait()


# ---------------------------------------------------------------------------
# (a) A scheduled task fires with ZERO HTTP requests to the server.
#
# This is the headline behavior fix (ticket 08-port-cron.md): under the old
# asgi_wrapper, the scheduler loop only started on the first non-lifespan
# request. A served instance that never received traffic ran no tasks --
# documented as an operational caveat in README.md. Under the new
# datasette.add_background_task() API the loop launches once every plugin's
# startup hook has completed, as part of ASGI lifespan startup -- strictly
# before uvicorn even starts accepting connections, let alone before any
# request arrives.
# ---------------------------------------------------------------------------

ZERO_HTTP_MARKER_PLUGIN_TEMPLATE = '''
import sqlite3
from datasette import hookimpl

MARKER_DB_PATH = {marker_db_path!r}


async def write_marker(datasette, config):
    # Bypasses Datasette's db/ASGI machinery entirely -- a raw stdlib
    # sqlite3 write, so this test's proof has nothing to do with whether
    # any HTTP request has ever reached the server.
    conn = sqlite3.connect(MARKER_DB_PATH)
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS ticks (id INTEGER PRIMARY KEY, at TEXT)"
        )
        conn.execute(
            "INSERT INTO ticks (at) VALUES (strftime('%Y-%m-%dT%H:%M:%f', 'now'))"
        )
        conn.commit()
    finally:
        conn.close()


@hookimpl
def cron_register_handlers(datasette):
    return {{"write-marker": write_marker}}


@hookimpl
def startup(datasette):
    async def inner():
        scheduler = datasette._cron_scheduler
        await scheduler.add_task(
            name="marker-every-second",
            handler="zero_http_marker_plugin:write-marker",
            schedule={{"interval": 1}},
            overlap="skip",
        )

    return inner
'''


def _tick_count(marker_db_path):
    if not marker_db_path.exists():
        return 0
    conn = sqlite3.connect(str(marker_db_path))
    try:
        try:
            return conn.execute("SELECT count(*) FROM ticks").fetchone()[0]
        except sqlite3.OperationalError:
            return 0
    finally:
        conn.close()


def test_task_fires_with_zero_http_requests(tmp_path):
    """A 1s-interval task must fire repeatedly on a served instance that
    never receives a single HTTP request -- proof is out-of-band (a raw
    sqlite file the test polls directly), so no readiness-poll request or
    `datasette.client` call is anywhere in this test, deliberately."""
    marker_db_path = tmp_path / "ticks.db"
    plugins_dir = tmp_path / "plugins"
    plugins_dir.mkdir()
    (plugins_dir / "zero_http_marker_plugin.py").write_text(
        ZERO_HTTP_MARKER_PLUGIN_TEMPLATE.format(marker_db_path=str(marker_db_path)),
        "utf-8",
    )

    port = _find_free_port()
    ds_proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "datasette",
            "--memory",
            "--plugins-dir",
            str(plugins_dir),
            "-p",
            str(port),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    try:
        _poll_until(
            lambda: _tick_count(marker_db_path) >= 2,
            timeout=15.0,
            proc=ds_proc,
            on_timeout_msg=(
                "Scheduled task never fired without any HTTP request "
                f"(ticks so far: {_tick_count(marker_db_path)})"
            ),
        )
    finally:
        _terminate(ds_proc)


# ---------------------------------------------------------------------------
# (b) SIGTERM -> shutdown hook runs -> in-flight run rows are finalized.
#
# The `shutdown` hookimpl (async def shutdown(datasette): await
# scheduler.shutdown()) runs BEFORE core cancels the supervised loop task
# and closes the internal database. Scheduler.shutdown() cancels every
# in-flight execution, and _execute_task's CancelledError handler records
# that as a finished run (status='error', error_message='Cancelled') before
# re-raising -- so a run that was genuinely in progress at shutdown time
# must not be left stuck at status='running' forever.
# ---------------------------------------------------------------------------

SIGTERM_FINALIZE_PLUGIN = '''
import asyncio
from datasette import hookimpl


async def slow_handler(datasette, config):
    await asyncio.sleep(30)


@hookimpl
def cron_register_handlers(datasette):
    return {"slow": slow_handler}


@hookimpl
def startup(datasette):
    async def inner():
        scheduler = datasette._cron_scheduler
        await scheduler.add_task(
            name="slow-task",
            handler="sigterm_finalize_plugin:slow",
            schedule={"interval": 1},
            overlap="skip",
        )

    return inner
'''


def _latest_run_status(internal_db_path):
    """(status, error_message) of the newest run row for slow-task, or
    None if the table/row doesn't exist yet or the db is transiently
    locked by the server process's own writer."""
    if not internal_db_path.exists():
        return None
    try:
        conn = sqlite3.connect(f"file:{internal_db_path}?mode=ro", uri=True)
    except sqlite3.OperationalError:
        return None
    try:
        try:
            return conn.execute(
                "SELECT status, error_message FROM datasette_cron_runs "
                "WHERE task_name = 'slow-task' ORDER BY id DESC LIMIT 1"
            ).fetchone()
        except sqlite3.OperationalError:
            return None
    finally:
        conn.close()


def test_sigterm_finalizes_in_flight_run(tmp_path):
    internal_db_path = tmp_path / "internal.db"
    plugins_dir = tmp_path / "plugins"
    plugins_dir.mkdir()
    (plugins_dir / "sigterm_finalize_plugin.py").write_text(
        SIGTERM_FINALIZE_PLUGIN, "utf-8"
    )

    port = _find_free_port()
    ds_proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "datasette",
            "--memory",
            "--internal",
            str(internal_db_path),
            "--plugins-dir",
            str(plugins_dir),
            "-p",
            str(port),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    try:
        # Wait until the slow execution has genuinely started (status
        # 'running'), so SIGTERM is guaranteed to land mid-flight.
        _poll_until(
            lambda: (_latest_run_status(internal_db_path) or (None,))[0]
            == "running",
            timeout=15.0,
            proc=ds_proc,
            on_timeout_msg="slow-task never reached status='running'",
        )

        ds_proc.send_signal(signal.SIGTERM)
        try:
            ds_proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            ds_proc.kill()
            ds_proc.wait()
            raise AssertionError(
                "datasette serve did not exit within 10s of SIGTERM\n"
                + ds_proc.stdout.read().decode("utf-8")
            )

        row = _latest_run_status(internal_db_path)
        assert row is not None, "no run row found for slow-task after exit"
        status, error_message = row
        assert status != "running", (
            "run row still shows status='running' after graceful shutdown "
            f"(error_message={error_message!r}) -- the shutdown hook did "
            "not finalize the in-flight execution before the process exited"
        )
    finally:
        _terminate(ds_proc)
