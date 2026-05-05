import argparse
import os
import shutil
import signal
import subprocess
import sys
from collections.abc import Sequence


def _spawn(command: Sequence[str], env: dict[str, str]) -> subprocess.Popen[str]:
    return subprocess.Popen(  # noqa: S603
        command,
        env=env,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run NightWatch demo stack (API + Worker + MCP + Web)")
    parser.add_argument("--api-host", default="127.0.0.1")
    parser.add_argument("--api-port", type=int, default=8000)
    parser.add_argument("--mcp-host", default="127.0.0.1")
    parser.add_argument("--mcp-port", type=int, default=9001)
    parser.add_argument("--web-host", default="127.0.0.1")
    parser.add_argument("--web-port", type=int, default=3000)
    parser.add_argument("--mcp-transport", default="streamable-http")
    args = parser.parse_args()

    root_env = os.environ.copy()

    api_command = [
        sys.executable,
        "-m",
        "uvicorn",
        "nightwatch.main:app",
        "--host",
        args.api_host,
        "--port",
        str(args.api_port),
    ]

    mcp_env = root_env.copy()
    mcp_env["NW_MCP_TRANSPORT"] = args.mcp_transport
    mcp_env["NW_MCP_HOST"] = args.mcp_host
    mcp_env["NW_MCP_PORT"] = str(args.mcp_port)
    mcp_command = [sys.executable, "-m", "nightwatch.mcp.server"]

    worker_command = [sys.executable, "-m", "nightwatch.worker"]

    web_env = root_env.copy()
    web_env["HOSTNAME"] = args.web_host
    web_env["PORT"] = str(args.web_port)

    npm_command = shutil.which("npm") or shutil.which("npm.cmd")
    if npm_command is None:
        raise RuntimeError("npm is required to run the Next.js web console")

    web_command = [
        npm_command,
        "run",
        "dev",
        "--",
        "-p",
        str(args.web_port),
        "-H",
        args.web_host,
    ]

    processes = [
        _spawn(api_command, env=root_env),
        _spawn(worker_command, env=root_env),
        _spawn(mcp_command, env=mcp_env),
        _spawn(web_command, env=web_env),
    ]

    try:
        for process in processes:
            process.wait()
    except KeyboardInterrupt:
        for process in processes:
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
        for process in processes:
            if process.poll() is None:
                process.terminate()


if __name__ == "__main__":
    main()
