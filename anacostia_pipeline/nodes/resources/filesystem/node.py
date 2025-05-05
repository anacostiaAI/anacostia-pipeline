import os
from typing import List, Any, Union
from datetime import datetime
from logging import Logger
from threading import Thread
import traceback
import time
from abc import ABC, abstractmethod
import asyncio
import httpx

from anacostia_pipeline.nodes.resources.node import BaseResourceNode
from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.nodes.metadata.rpc import BaseMetadataRPCCaller
from anacostia_pipeline.nodes.resources.filesystem.gui import FilesystemStoreGUI
from anacostia_pipeline.nodes.resources.filesystem.rpc import FilesystemStoreRPCCallee



class FilesystemStoreNode(BaseResourceNode, ABC):
    def __init__(
        self, 
        name: str, 
        resource_path: str, 
        metadata_store: BaseMetadataStoreNode = None,
        metadata_store_caller: BaseMetadataRPCCaller = None,
        init_state: str = "new", 
        max_old_samples: int = None, 
        remote_predecessors: List[str] = None,
        remote_successors: List[str] = None,
        caller_url: str = None,
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
            metadata_store_caller=metadata_store_caller,
            remote_predecessors=remote_predecessors,
            remote_successors=remote_successors,
            caller_url=caller_url,
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
            metadata_store_caller=self.metadata_store_caller
        )
        return self.gui
    
    def setup_rpc_callee(self, host, port):
        self.rpc_callee = FilesystemStoreRPCCallee(self, self.caller_url, host, port, loggers=self.loggers)
        return self.rpc_callee

    def start_monitoring(self) -> None:

        async def _monitor_thread_func():
            self.log(f"Starting observer thread for node '{self.name}'")
            while self.exit_event.is_set() is False:
                for root, dirnames, filenames in os.walk(self.path):
                    for filename in filenames:
                        filepath = os.path.join(root, filename)
                        filepath = filepath.removeprefix(self.path)     # Remove the path prefix
                        filepath = filepath.lstrip(os.sep)              # Remove leading separator

                        try:
                            entry_exists = await self.entry_exists(filepath) 
                            if entry_exists is False:
                                await self.record_new(filepath)
                                self.log(f"detected file {filepath}", level="INFO")
                        
                        except Exception as e:
                            self.log(f"Unexpected error in monitoring logic for '{self.name}': {traceback.format_exc()}", level="ERROR")

                if self.exit_event.is_set() is True: break
                try:
                    await self.resource_trigger()
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

    async def resource_trigger(self) -> None:
        """
        The default trigger for the FilesystemStoreNode. 
        resource_trigger checks if there are any new files in the resource directory and triggers the node if there are.
        """
        num_new_artifacts = await self.get_num_artifacts("new")
        if num_new_artifacts > 0:
            await self.trigger(message=f"New files detected in {self.resource_path}")
    
    def _save_artifact_hook(self, filepath: str, *args, **kwargs) -> None:
        """
        This method should be overridden by the user to implement the logic for saving an artifact.
        The method should accept the filepath as the first argument and any additional arguments or keyword arguments as needed.
        The method should raise an exception if the save operation fails.
        This method is called by the save_artifact method.
        Args:
            filepath (str): Path of the file to save relative to the resource_path. Example: "data/file.txt" will save the file at resource_path/data/file.txt.
            *args: Additional positional arguments for the function.
            **kwargs: Additional keyword arguments for the function.
        Raises:
            NotImplementedError: If the method is not implemented by the user.
            Exception: If the save operation fails.
        """
        pass

    async def save_artifact(self, filepath: str, *args, **kwargs):
        """
        Save a file using the provided function and filepath.
        
        Args:
            filepath (str): Path of the file to save relative to the resource_path. Example: "data/file.txt" will save the file at resource_path/data/file.txt.
            *args: Additional positional arguments for the function.
            **kwargs: Additional keyword arguments for the function.
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
            # await self.record_current(filepath)

            self._save_artifact_hook(artifact_save_path, *args, **kwargs)
            await self.record_current(filepath)
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
    
    def _load_artifact_hook(self, filepath: str, *args, **kwargs) -> Any:
        """
        This method should be overridden by the user to implement the logic for loading an artifact.
        The method should accept the filepath as the first argument and any additional arguments or keyword arguments as needed.
        The method should raise an exception if the load operation fails.
        This method is called by the load_artifact method.
        Args:
            filepath (str): Path of the file to load relative to the resource_path. Example: "data/file.txt" will load the file at resource_path/data/file.txt.
            *args: Additional positional arguments for the function.
            **kwargs: Additional keyword arguments for the function.
        Raises:
            NotImplementedError: If the method is not implemented by the user.
            Exception: If the load operation fails.
        """
        pass

    def load_artifact(self, filepath: str, *args, **kwargs) -> Any:
        """
        Load an artifact from the specified path inside to the resource_path.
        Args:
            artifact_path (str): The path of the artifact to load, inside to the resource_path. Example: "data/file.txt" will load the file at resource_path/data/file.txt.
            *args: Additional positional arguments for the function.
            **kwargs: Additional keyword arguments for the function.
        Returns:
            Any: The loaded artifact.
        Raises:
            FileNotFoundError: If the artifact file does not exist.
        """
        
        artifact_save_path = os.path.join(self.resource_path, filepath)
        if os.path.exists(artifact_save_path) is False:
            raise FileExistsError(f"File '{artifact_save_path}' does not exists.")

        try:
            return self._load_artifact_hook(artifact_save_path, *args, **kwargs)
        except Exception as e:
            self.log(f"Failed to load artifact '{filepath}': {e}", level="ERROR")
            raise e

    def stop_monitoring(self) -> None:
        self.log(f"Beginning teardown for node '{self.name}'")
        self.observer_thread.join()
        self.log(f"Observer stopped for node '{self.name}'")