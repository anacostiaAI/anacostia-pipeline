import os
from typing import List, Any, Union, Callable, Iterator
from contextlib import contextmanager, ExitStack
from datetime import datetime
from logging import Logger
from threading import Thread
import traceback
import time
from abc import ABC
import hashlib
import tempfile

from anacostia_pipeline.nodes.resources.node import BaseResourceNode
from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.nodes.metadata.api import BaseMetadataStoreClient
from anacostia_pipeline.nodes.api import NetworkConnectionNotEstablished
from anacostia_pipeline.nodes.resources.filesystem.gui import FilesystemStoreGUI
from anacostia_pipeline.nodes.resources.filesystem.api import FilesystemStoreServer



class FilesystemStoreNode(BaseResourceNode, ABC):
    def __init__(
        self, 
        name: str, 
        resource_path: str, 
        metadata_store: BaseMetadataStoreNode = None,
        metadata_store_client: BaseMetadataStoreClient = None,
        init_state: str = "new", 
        max_old_samples: int = None, 
        remote_predecessors: List[str] = None,
        remote_successors: List[str] = None,
        client_url: str = None,
        wait_for_connection: bool = False,
        loggers: Union[Logger, List[Logger]] = None, 
        monitoring: bool = True
    ) -> None:

        # TODO: add max_old_samples functionality
        self.max_old_samples = max_old_samples
        
        # note: the resource_path must be a path for a directory.
        # we may want to rename this node to be a directory watch node;
        # this means this node should only be used to monitor filesystem directories and S3 buckets
        self.path = os.path.abspath(resource_path)
        if os.path.exists(self.path) is False:
            os.makedirs(self.path, exist_ok=True)
        
        self.observer_thread = None

        if init_state not in ("new", "old"):
            raise ValueError(f"init_state argument of DataStoreNode must be either 'new' or 'old', not '{init_state}'.")
        self.init_state = init_state
        self.init_time = str(datetime.now())
        
        super().__init__(
            name=name, 
            resource_path=resource_path, 
            metadata_store=metadata_store, 
            metadata_store_client=metadata_store_client,
            remote_predecessors=remote_predecessors,
            remote_successors=remote_successors,
            client_url=client_url,
            wait_for_connection=wait_for_connection,
            loggers=loggers, 
            monitoring=monitoring
        )
    
    def setup_node_GUI(self, host: str, port: int, ssl_keyfile: str = None, ssl_certfile: str = None, ssl_ca_certs: str = None) -> FilesystemStoreGUI:
        self.gui = FilesystemStoreGUI(
            node=self, 
            host=host,
            port=port,
            metadata_store=self.metadata_store, 
            metadata_store_client=self.metadata_store_client,
            ssl_keyfile=ssl_keyfile,
            ssl_certfile=ssl_certfile,
            ssl_ca_certs=ssl_ca_certs
        )
        return self.gui

    def setup_node_server(self, host: str, port: int, ssl_keyfile: str = None, ssl_certfile: str = None, ssl_ca_certs: str = None) -> FilesystemStoreServer:
        self.node_server = FilesystemStoreServer(
            self, self.client_url, host, port, loggers=self.loggers, ssl_keyfile=ssl_keyfile, ssl_certfile=ssl_certfile, ssl_ca_certs=ssl_ca_certs
        )
        return self.node_server

    def start_monitoring(self) -> None:

        def _monitor_thread_func():
            self.log(f"Starting observer thread for node '{self.name}'", level="INFO")
            while self.exit_event.is_set() is False:
                for root, dirnames, filenames in os.walk(self.path):
                    for filename in filenames:
                        filepath = os.path.join(root, filename)
                        
                        hash = self.hash_file(filepath)

                        filepath = filepath.removeprefix(self.path)     # Remove the path prefix
                        filepath = filepath.lstrip(os.sep)              # Remove leading separator

                        try:
                            entry_exists = self.entry_exists(filepath) 
                            if entry_exists is False:
                                self.record_new(filepath, hash=hash, hash_algorithm="sha256")
                                self.log(f"detected file {filepath}", level="INFO")
                        
                        except Exception as e:
                            self.log(f"Unexpected error in monitoring logic for '{self.name}': {traceback.format_exc()}", level="ERROR")

                if self.exit_event.is_set() is True: 
                    self.log(f"Observer thread for node '{self.name}' exiting", level="INFO")
                    return
                try:
                    self.resource_trigger()
                
                except NetworkConnectionNotEstablished as e:
                    pass

                except Exception as e:
                    self.log(f"Error checking resource in node '{self.name}': {traceback.format_exc()}", level="ERROR")
                    # Note: we continue here because we want to keep trying to check the resource until it is available
                    # with that said, we should add an option for the user to specify the number of times to try before giving up
                    # and throwing an exception
                    # Note: we also continue because we don't want to stop checking in the case of a corrupted file or something like that. 
                    # We should also think about adding an option for the user to specify what actions to take in the case of an exception,
                    # e.g., send an email to the data science team to let everyone know the resource is corrupted, 
                    # or just not move the file to current.
                
                # sleep for a while before checking again
                time.sleep(0.1)

            self.log(f"Observer thread for node '{self.name}' exited", level="INFO")

        # since we are using asyncio.run, we need to create a new thread to run the event loop 
        # because we can't run an event loop in the same thread as the FilesystemStoreNode
        self.observer_thread = Thread(name=f"{self.name}_observer", target=_monitor_thread_func, daemon=True)
        self.observer_thread.start()

    def hash_file(self, filepath: str, chunk_size: int = 8192) -> str:
        sha256 = hashlib.sha256()
        with open(filepath, 'rb') as f:
            while chunk := f.read(chunk_size):
                sha256.update(chunk)
        return sha256.hexdigest()

    def resource_trigger(self) -> None:
        """
        The default trigger for the FilesystemStoreNode. 
        resource_trigger checks if there are any new files in the resource directory and triggers the node if there are.
        """

        num_new_artifacts = self.get_num_artifacts("new")
        if num_new_artifacts is not None:
            if num_new_artifacts > 0:
                self.trigger(message=f"New files detected in {self.resource_path}")

    def get_num_artifacts(self, state: str) -> int:
        """
        Get the number of artifacts in the specified state.
        
        Args:
            state: The state of the artifacts to count (e.g., "new", "current", "old")
        
        Returns:
            int: The number of artifacts in the specified state
        """

        if self.metadata_store is not None:
            return self.metadata_store.get_num_entries(self.name, state)
        
        if self.metadata_store_client is not None:
            if self.connection_event.is_set() is True:
                return self.metadata_store_client.get_num_entries(self.name, state)

    def list_artifacts(self, state: str) -> List[str]:
        """
        List all artifacts in the resource path.
        Args:
            state (str): The state of the artifacts to list. Can be "new" or "old".
        Returns:
            List[str]: A list of artifact paths.
        """

        entries = super().list_artifacts(state)
        full_artifacts_paths = [os.path.join(self.path, entry) for entry in entries]
        return full_artifacts_paths
    
    def get_run_id(self) -> List[str]:
        if self.metadata_store is not None:
            return self.metadata_store.get_run_id()

        if self.metadata_store_client is not None:
            if self.connection_event.is_set() is True:
                self.metadata_store_client.get_run_id()

    @contextmanager
    def save_artifact(
        self,
        filepath: str,
        save_fn: Callable[[str, Any], Any],
        *args,
        overwrite: bool = False,
        atomic: bool = True,
        **kwargs
    ) -> Iterator[Any]:
        """
        Context manager to save artifacts.

        Args:
            filepath: path relative to self.resource_path
            save_fn: function that takes (target_path, *args, **kwargs) and returns either:
                - a context manager, OR
                - an object with .close(), OR
                - a plain object / None
            *args: additional positional arguments to pass to `save_fn`
            overwrite: if False (default), raise if destination exists
            atomic: if True (default), write to a temp file and os.replace() into place on success
            **kwargs: additional keyword arguments to pass to `save_fn`

        ## Usage patterns:
        1. “Fire and forget” writer (returns None) 
        ```
        fs_store = FilesystemStoreNode(...)

        def write_text(path, text: str):
            with open(path, "w", encoding="utf-8") as f:
                f.write(text)

        with fs_store.save_artifact_cm("notes/hello.txt", write_text, text="Hello, world!"):
            pass  # nothing else to do
        ```
        2. Return an open file handle and write inside the `with` block
        ```
        fs_store = FilesystemStoreNode(...)

        def open_writer(path):
            return open(path, "w", encoding="utf-8")

        with fs_store.save_artifact_cm("logs/run.log", open_writer) as f:
            f.write("first line\\n")
            f.write("second line\\n")
        ```
        3. Save a PIL image (write immediately)
        ```
        from PIL import Image

        fs_store = FilesystemStoreNode(...)

        def save_image(path, img: Image.Image, fmt="PNG"):
            img.save(path, format=fmt)

        img = Image.new("RGB", (100, 100))
        with fs_store.save_artifact_cm("images/blank.png", save_image, img, fmt="PNG"):
            pass
        ```
        4. Overwrite an existing file atomically
        ```
        fs_store = FilesystemStoreNode(...)

        with fs_store.save_artifact_cm("notes/hello.txt", write_text, text="Updated", overwrite=True):
            pass
        ```
        5. Appending to an existing file (non-atomic)
        ```
        fs_store = FilesystemStoreNode(...)
        
        def append_line(path: str, line: str) -> None:
            with open(path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
                f.flush()
                os.fsync(f.fileno())

        with fs_store.save_artifact_cm(
            "logs/service.log",
            append_line,
            "service started",
            atomic=False,
            overwrite=True   # file likely exists; we're appending to it
        ):
            pass
        ```
        """

        if self.monitoring is True:
            raise ValueError(
                "Cannot save artifact while monitoring is enabled. "
                "Please disable monitoring before saving artifacts."
            )

        folder_path = os.path.join(self.resource_path, os.path.dirname(filepath))
        os.makedirs(folder_path, exist_ok=True)

        artifact_path = os.path.join(self.resource_path, filepath)

        if os.path.exists(artifact_path) and not overwrite:
            raise FileExistsError(
                f"File '{artifact_path}' already exists. "
                f"Use overwrite=True or choose a different filename."
            )

        # Choose a write target: direct path or a temp file for atomic replace.
        tmp_path = artifact_path
        tmp_created = False
        if atomic:
            # Create a unique temp file in the same directory (safe across crashes).
            base = os.path.basename(artifact_path)
            fd, tmp_path = tempfile.mkstemp(
                dir=folder_path, prefix=f".{base}.", suffix=".tmp"
            )
            os.close(fd)
            tmp_created = True

        try:
            # Let the user-provided function set up the write target.
            obj = save_fn(tmp_path, *args, **kwargs)

            # Manage whatever save_fn returned (CM, closeable, or plain)
            with ExitStack() as stack:
                if hasattr(obj, "__enter__") and hasattr(obj, "__exit__"):
                    resource = stack.enter_context(obj)  # type: ignore[arg-type]
                    yield resource
                else:
                    if hasattr(obj, "close") and callable(getattr(obj, "close")):
                        stack.callback(obj.close)
                    # Even if obj is None, yielding None is fine; callers can `pass`.
                    yield obj

            # Commit: move temp file into place atomically (or nothing if non-atomic)
            if atomic:
                os.replace(tmp_path, artifact_path)

            # Hash and record after the file is finalized
            file_hash = self.hash_file(artifact_path)
            self.record_produced_artifact(filepath, hash=file_hash, hash_algorithm="sha256")
            self.log(f"Saved artifact to {artifact_path}", level="INFO")

        except Exception as e:
            # Best-effort cleanup of temp file on failure
            if atomic and tmp_created and os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception as cleanup_err:
                    self.log(
                        f"Cleanup warning: could not remove temp file '{tmp_path}': {cleanup_err}",
                        level="WARNING",
                    )
            self.log(f"Failed to save artifact '{filepath}': {e}", level="ERROR")
            raise

    @contextmanager
    def load_artifact(self, filepath: str, load_fn: Callable[[str, Any], Any], *args, **kwargs) -> Iterator[Any]:
        """
        Context manager to load an artifact from the specified path relative to the resource_path.

        Args:
            filepath (str): Path of the artifact to load, relative to the resource_path.
                            Example: "data/file.txt" will load the file at resource_path/data/file.txt.
            load_fn (Callable[[str, Any], Any]): A function that takes the full path to the artifact and additional arguments,
                                                 and returns the loaded artifact.
            *args: Additional positional arguments to pass to `load_fn`.
            **kwargs: Additional keyword arguments to pass to `load_fn`.

        Returns:
            Any: The loaded artifact.

        Raises:
            FileNotFoundError: If the artifact file does not exist.
            Exception: If an error occurs during loading.
        
        ## Usage patterns:
        1. Loading a file
        ```
        fs_store = FilesystemStoreNode(...)

        with fs_store.load_artifact_cm("data/file.txt", open, mode="r") as f:
            buf = f.read()
        ```
        2. Loading a PyTorch model
        ```
        import torch
        
        fs_store = FilesystemStoreNode(...)

        with fs_store.load_artifact_cm("models/model.pt", torch.load, map_location='cpu') as model:
            # use the model here
            model.eval()
            ...
        ```
        3. Using a custom load function
        ```
        fs_store = FilesystemStoreNode(...)

        def load_fn(input_file_path) -> str:
            with open(input_file_path, "r", encoding="utf-8") as f:
                return f.read()

        with fs_store.load_artifact_cm("data/readme.txt", load_fn) as text:
            print("First 120 chars:", text[:120])
        ```
        """

        artifact_path = os.path.join(self.resource_path, filepath)
        if not os.path.exists(artifact_path):
            raise FileNotFoundError(f"File '{artifact_path}' does not exist.")

        try:
            actual_hash = self.hash_file(artifact_path)
            relative_path = os.path.relpath(artifact_path, self.resource_path)
            expected_hash = self.get_artifact_hash(relative_path)

            if expected_hash != actual_hash:
                self.log(
                    f"Warning: hash mismatch for '{filepath}': expected {expected_hash}, got {actual_hash}",
                    level="WARNING"
                )
 
            self.mark_using(relative_path)

            obj = load_fn(artifact_path, *args, **kwargs)

            with ExitStack() as stack:
                # If it's already a context manager, enter it.
                if hasattr(obj, "__enter__") and hasattr(obj, "__exit__"):
                    resource = stack.enter_context(obj)  # type: ignore[arg-type]
                    yield resource
                else:
                    # If it exposes .close(), make sure we close it on exit.
                    if hasattr(obj, "close") and callable(getattr(obj, "close")):
                        stack.callback(obj.close)
                    yield obj
                # ExitStack ensures cleanup happens here.

            self.mark_used(relative_path)

        except Exception as e:
            self.log(f"Failed to load artifact '{filepath}': {e}", level="ERROR")
            raise

    def stop_monitoring(self) -> None:
        self.log(f"Stopping observer thread for node '{self.name}'", level="INFO")
        self.observer_thread.join()
        self.log(f"Observer stopped for node '{self.name}'", level="INFO")