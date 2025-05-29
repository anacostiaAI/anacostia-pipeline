import os
from typing import List, Any, Union, Callable
from datetime import datetime
from logging import Logger
from threading import Thread
import traceback
import time
from abc import ABC
import asyncio
import hashlib
import inspect

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
    
    def setup_node_GUI(self, host: str, port: int) -> FilesystemStoreGUI:
        self.gui = FilesystemStoreGUI(
            node=self, 
            host=host,
            port=port,
            metadata_store=self.metadata_store, 
            metadata_store_client=self.metadata_store_client
        )
        return self.gui
    
    def setup_node_server(self, host: str, port: int):
        self.node_server = FilesystemStoreServer(self, self.client_url, host, port, loggers=self.loggers)
        return self.node_server

    def start_monitoring(self) -> None:

        async def _monitor_thread_func():
            self.log(f"Starting observer thread for node '{self.name}'")
            while self.exit_event.is_set() is False:
                for root, dirnames, filenames in os.walk(self.path):
                    for filename in filenames:
                        filepath = os.path.join(root, filename)
                        
                        hash = self.hash_file(filepath)

                        filepath = filepath.removeprefix(self.path)     # Remove the path prefix
                        filepath = filepath.lstrip(os.sep)              # Remove leading separator

                        try:
                            entry_exists = await self.entry_exists(filepath) 
                            if entry_exists is False:
                                await self.record_new(filepath, hash=hash, hash_algorithm="sha256")
                                self.log(f"detected file {filepath}", level="INFO")
                        
                        except Exception as e:
                            self.log(f"Unexpected error in monitoring logic for '{self.name}': {traceback.format_exc()}", level="ERROR")

                if self.exit_event.is_set() is True: break
                try:
                    await self.resource_trigger()
                
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

        # since we are using asyncio.run, we need to create a new thread to run the event loop 
        # because we can't run an event loop in the same thread as the FilesystemStoreNode
        self.observer_thread = Thread(name=f"{self.name}_observer", target=asyncio.run, args=(_monitor_thread_func(),))
        self.observer_thread.start()

    def hash_file(self, filepath: str, chunk_size: int = 8192) -> str:
        sha256 = hashlib.sha256()
        with open(filepath, 'rb') as f:
            while chunk := f.read(chunk_size):
                sha256.update(chunk)
        return sha256.hexdigest()

    async def resource_trigger(self) -> None:
        """
        The default trigger for the FilesystemStoreNode. 
        resource_trigger checks if there are any new files in the resource directory and triggers the node if there are.
        """
        num_new_artifacts = await self.get_num_artifacts("new")
        if num_new_artifacts > 0:
            await self.trigger(message=f"New files detected in {self.resource_path}")
    
    async def save_artifact(self, filepath: str, save_fn: Callable[[str, Any], None], *args, **kwargs):
        """
        Save a file using the provided function and filepath.

        Args:
            filepath (str): Path of the file to save relative to the resource_path.
                            Example: "data/file.txt" will save the file at resource_path/data/file.txt.
            save_fn (Callable): A function that takes the full save path as its first argument,
                                followed by *args and **kwargs. It is responsible for writing the file.
            *args: Additional positional arguments passed to `save_fn`.
            **kwargs: Additional keyword arguments passed to `save_fn`.
        """

        # as of right now, i am not going to allow monitoring resource nodes to be used for detecting new data,
        # i haven't seen a use case where it's necessary to save a file while monitoring is enabled.
        if self.monitoring is True:
            raise ValueError("Cannot save artifact while monitoring is enabled. Please disable monitoring before saving artifacts.")

        # Ensure the root directory exists
        folder_path = os.path.join(self.resource_path, os.path.dirname(filepath))
        if os.path.exists(folder_path) is False:
            os.makedirs(folder_path)
        
        artifact_save_path = os.path.join(self.resource_path, filepath)
        if os.path.exists(artifact_save_path) is True:
            raise FileExistsError(f"File '{artifact_save_path}' already exists. Please choose a different filename.")

        try:
            # note: for monitoring-enabled resource nodes, record_artifact should be called before create_file;
            # that way, the Observer can see the file is already logged and ignore it.
            # await self.record_current(filepath, hash=hash, hash_algorithm="sha256")

            # write the file to artifact_save_path using the user-provided function save_fn
            save_fn(artifact_save_path, *args, **kwargs)

            hash = self.hash_file(artifact_save_path)

            # TODO: change this to self.add_artifact, not every artifact will be saved as a current artifact
            await self.record_current(filepath, hash=hash, hash_algorithm="sha256")
            self.log(f"Saved artifact to {artifact_save_path}", level="INFO")

        except Exception as e:
            self.log(f"Failed to save artifact '{filepath}': {e}", level="ERROR")
            raise e

    async def list_artifacts(self, state: str) -> List[str]:
        """
        List all artifacts in the resource path.
        Args:
            state (str): The state of the artifacts to list. Can be "new" or "old".
        Returns:
            List[str]: A list of artifact paths.
        """

        entries = await super().list_artifacts(state)
        full_artifacts_paths = [os.path.join(self.path, entry) for entry in entries]
        return full_artifacts_paths

    def load_artifact(self, filepath: str, load_fn: Callable[[str, Any], Any], *args, **kwargs) -> Any:
        """
        Load an artifact from the specified path relative to the resource_path.

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
        """
        
        artifact_save_path = os.path.join(self.resource_path, filepath)
        if os.path.exists(artifact_save_path) is False:
            raise FileExistsError(f"File '{artifact_save_path}' does not exists.")

        try:
            actual_hash = self.hash_file(artifact_save_path)
            expected_hash = None
            # TODO: get hash from metadata store and compare the expected hash with actual_hash

            artifact = load_fn(artifact_save_path, *args, **kwargs)
            return artifact
        except Exception as e:
            self.log(f"Failed to load artifact '{filepath}': {e}", level="ERROR")
            raise e

    def stop_monitoring(self) -> None:
        self.log(f"Beginning teardown for node '{self.name}'")
        self.observer_thread.join()
        self.log(f"Observer stopped for node '{self.name}'")