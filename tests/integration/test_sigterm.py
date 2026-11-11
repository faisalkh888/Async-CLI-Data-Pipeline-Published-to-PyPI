import json
import os
import signal
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest


def test_sigterm_handler_cancels_pipeline_on_all_platforms():
    import asyncio

    from datapipe import cli
    async def cancel_task():
        loop = asyncio.get_running_loop()

        async def wait_forever():
            await asyncio.Event().wait()

        task = asyncio.create_task(wait_forever())
        cli._sigterm_canceller(loop, task)(signal.SIGTERM, None)
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(cancel_task())


@pytest.mark.skipif(os.name == "nt", reason="Windows does not deliver POSIX SIGTERM to child processes")
def test_sigterm_discards_staged_output_and_rerun_commits(tmp_path):
    second_page_entered = threading.Event()
    release_second_page = threading.Event()
    state = {"second_page_requests": 0}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            page = parse_qs(urlparse(self.path).query).get("page", ["1"])[0]
            if page == "1":
                payload = {"records": [{"id": 1}], "next": None}
            else:
                state["second_page_requests"] += 1
                second_page_entered.set()
                if state["second_page_requests"] == 1:
                    release_second_page.wait(timeout=10)
                payload = {"records": [], "next": None}
            body = json.dumps(payload).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def log_message(self, _format, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    output_dir = tmp_path / "output"
    checkpoint = tmp_path / "checkpoint.json"
    config_path = tmp_path / "pipeline.toml"
    config_path.write_text(
        f'''[pipeline]
name = "sigterm-recovery"
output_dir = {json.dumps(str(output_dir))}

[settings]
queue_size = 1
batch_size = 1
request_timeout = 20
max_retries = 0
checkpoint_file = {json.dumps(str(checkpoint))}

[[sources]]
name = "delayed-api"
type = "http"
url = "http://127.0.0.1:{server.server_port}/records"
pagination = "page_number"
page_param = "page"
records_path = "records"
max_pages = 3

[[sinks]]
name = "records"
type = "jsonl"
path = "records.jsonl"
''',
        encoding="utf-8",
    )
    project_root = Path(__file__).resolve().parents[2]
    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join(
        filter(None, [str(project_root / "src"), environment.get("PYTHONPATH")])
    )
    command = [sys.executable, "-m", "datapipe", "run", str(config_path)]
    process = subprocess.Popen(command, cwd=project_root, env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        assert second_page_entered.wait(timeout=10), "pipeline did not reach the delayed second page"
        temp_files = list(output_dir.glob(".records.jsonl.*.tmp"))
        deadline = time.monotonic() + 5
        while not temp_files and time.monotonic() < deadline:
            time.sleep(0.05)
            temp_files = list(output_dir.glob(".records.jsonl.*.tmp"))
        assert temp_files, "sink did not create its staged output before SIGTERM"

        process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=10)
        assert process.returncode == 130, (stdout.decode(errors="replace"), stderr.decode(errors="replace"))
        assert not (output_dir / "records.jsonl").exists()
        assert not list(output_dir.glob("*.tmp"))
        assert not checkpoint.exists()

        second_page_entered.clear()
        rerun = subprocess.run(
            [*command, "--resume"], check=False, cwd=project_root, env=environment,
            capture_output=True, text=True, timeout=20,
        )
        assert rerun.returncode == 0, rerun.stderr
        rows = [json.loads(line) for line in (output_dir / "records.jsonl").read_text(encoding="utf-8").splitlines()]
        assert rows == [{"id": 1}]
        assert checkpoint.exists()
    finally:
        release_second_page.set()
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=5)
