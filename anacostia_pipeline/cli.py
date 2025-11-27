# anacostia/cli.py

import os
import sys
import time
import subprocess
from pathlib import Path
import argparse
import importlib
import inspect
import uvicorn

from anacostia_pipeline.pipelines.server import PipelineServer
from anacostia_pipeline.tutorial import tutorial_app



# Environment variable to distinguish parent vs child
_CHILD_ENV_VAR = "ANACOSTIA_RELOADER_CHILD"


def _run_default_app(host: str = "127.0.0.1", port: int = 8000):
    # Call the target (e.g., `run()`)
    uvicorn.run(
        tutorial_app,
        host=host,
        port=port,
        reload=False,        # we already have our own reloader
    )


def _run_app(app_path: str, host: str = "127.0.0.1", port: int = 8000):
    """
    Import and run the app specified by `app_path`.

    `app_path` should be of the form 'module.submodule:attr',
    where `attr` is a callable (e.g., a function like `run`).
    """
    
    # add current dir to sys.path to allow local imports
    repo_dir = Path.cwd()
    if repo_dir.is_dir():
        sys.path.insert(0, str(repo_dir))

    try:
        module_name, attr_name = app_path.split(":", 1)
    except ValueError:
        raise SystemExit(
            f"Invalid --app value '{app_path}'. Expected format 'module:attr', e.g. 'myproj.main:run'."
        )

    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise SystemExit(f"Could not import module '{module_name}' for --app: {exc}") from exc

    try:
        fastapi_app = getattr(module, attr_name)
        if not isinstance(fastapi_app, PipelineServer):
            raise TypeError(f"Attribute {attr_name} in module {module_name} is not a PipelineServer instance, got {type(fastapi_app)}")

    except AttributeError as exc:
        raise SystemExit(
            f"Module '{module_name}' has no attribute '{attr_name}' (from --app '{app_path}')."
        ) from exc
    
    except TypeError as exc:
        raise SystemExit(str(exc)) from exc

    # Call the target (e.g., `run()`)
    uvicorn.run(
        app=fastapi_app,
        host=fastapi_app.host,
        port=fastapi_app.port,
        ssl_ca_certs=fastapi_app.ssl_ca_certs,
        ssl_certfile=fastapi_app.ssl_certfile,
        ssl_keyfile=fastapi_app.ssl_keyfile,
        log_config=fastapi_app.uvicorn_access_log_config,
        reload=False,        # we already have our own reloader
    )


def _iter_python_files(root: Path):
    """Yield all .py files under the given root directory."""
    for path in root.rglob("*.py"):
        yield path


def _snapshot_mtimes(root: Path):
    """Return a dict mapping file -> mtime for all .py files."""
    return {path: path.stat().st_mtime for path in _iter_python_files(root)}


def _resolve_app_directory(app_path: str) -> Path:
    # add current dir to sys.path to allow local imports
    repo_dir = Path.cwd()
    if repo_dir.is_dir():
        sys.path.insert(0, str(repo_dir))

    # Import the module part of the app path
    module_name, _ = app_path.split(":", 1)
    module = importlib.import_module(module_name)

    # Inspect the file the module came from and return its parent directory
    module_file = inspect.getfile(module)
    return Path(module_file).resolve().parent


def _run_with_reloader(args):
    """
    Parent process: spawn child that runs the app,
    watch for file changes, restart child on change.
    """
    package_root = _resolve_app_directory(args.app)
    print(f"Watching for changes under: {package_root}")

    # Initial snapshot of file mtimes
    mtimes = _snapshot_mtimes(package_root)

    child_args = []
    if args.host:
        child_args += ["--host", args.host]
    if args.port:
        child_args += ["--port", str(args.port)]

    while True:
        # Spawn child process
        env = os.environ.copy()
        env[_CHILD_ENV_VAR] = "1"  # mark as child

        # Build the command for the child.
        # We pass --app through so the child knows which app to run.
        # Use the package name (anacostia_pipeline) because -m runs a module's __main__;
        # the shell entrypoint `anacostia` works via setuptools, but there is no anacostia module to import.

        # anacostia (the shell command) works because setuptools creates a console script entrypoint from setup.py that 
        # points anacostia -> anacostia_pipeline.cli:main. When you type anacostia …, that stub script imports anacostia_pipeline for you.

        # python -m anacostia fails because -m looks for a module/package named exactly anacostia and runs its __main__.py. 
        # There is no anacostia package in the repo; the package is anacostia_pipeline (anacostia_pipeline/__main__.py). 
        # So the import blows up before your CLI code runs.
        cmd = [sys.executable, "-m", "anacostia_pipeline", "--app", args.app, *child_args]
        if args.no_reload:
            # Shouldn't happen because we're only here if reload=True,
            # but we keep args around for completeness.
            pass

        print("Starting child process...", " ".join(cmd))
        proc = subprocess.Popen(cmd, env=env)

        try:
            while True:
                # Has the child exited?
                return_code = proc.poll()
                if return_code is not None:
                    print(f"Child exited with code {return_code}.")
                    return return_code

                time.sleep(1.0)

                # Check for file changes
                new_mtimes = _snapshot_mtimes(package_root)
                if new_mtimes != mtimes:
                    print("Detected file change. Reloading...")
                    mtimes = new_mtimes
                    # Kill child and break to restart
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                    break  # restart loop (new child)
        except KeyboardInterrupt:
            print("Reloader got KeyboardInterrupt, shutting down.")
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
            return 0


def main():
    """
    Entrypoint for the `anacostia` command and `python -m anacostia`.
    Handles:
      - parent (reloader)
      - child (actual app run)
    """
    parser = argparse.ArgumentParser(description="Anacostia server with optional auto-reload.")
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Enable auto-reload on code changes.",
    )
    parser.add_argument(
        "--no-reload",
        action="store_true",
        help="Disable auto-reload (run app once).",
    )
    parser.add_argument(
        "--app",
        default=None,
        help=(
            "Application entrypoint as module:attr, "
            "e.g. 'some_repo.main:run'. Default is 'anacostia.app:run'."
        ),
    )
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind.")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind.")
    args = parser.parse_args()

    # If we are in the child process, just run the app once.
    if os.environ.get(_CHILD_ENV_VAR) == "1":
        _run_app(app_path=args.app, host=args.host, port=args.port)
        return

    # Parent process behavior
    if args.reload and not args.no_reload:
        # Parent acts as reloader
        print("Starting reloader...")
        exit_code = _run_with_reloader(args)
        sys.exit(exit_code)
    elif args.app is not None:
        # No reload: just run the app in this process
        _run_app(app_path=args.app, host=args.host, port=args.port)
    else:
        # No app specified: run default app
        _run_default_app(host=args.host, port=args.port)


if __name__ == "__main__":
    main()
