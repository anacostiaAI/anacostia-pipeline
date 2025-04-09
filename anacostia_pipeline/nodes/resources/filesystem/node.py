import os
from typing import List, Any, Union, Dict, Callable
from datetime import datetime
from logging import Logger
from threading import Thread
import traceback
import time

from anacostia_pipeline.nodes.resources.node import BaseResourceNode
from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.nodes.resources.filesystem.gui import FilesystemStoreGUI
from anacostia_pipeline.nodes.resources.filesystem.rpc import FilesystemStoreRPCCallee



class EntryNotFoundError(Exception):
    """Raised when an entry with specified ID is not found"""
    pass



class FilesystemStoreNode(BaseResourceNode):
    def __init__(
        self, 
        name: str, 
        resource_path: str, 
        metadata_store: BaseMetadataStoreNode, 
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
            remote_predecessors=remote_predecessors,
            remote_successors=remote_successors,
            caller_url=caller_url,
            wait_for_connection=wait_for_connection,
            loggers=loggers, 
            monitoring=monitoring
        )
    
    def setup_node_GUI(self) -> FilesystemStoreGUI:
        return FilesystemStoreGUI(self)
    
    def setup_rpc_callee(self, host, port):
        self.rpc_callee = FilesystemStoreRPCCallee(self, self.caller_url, host, port, loggers=self.loggers)
        return self.rpc_callee

    def setup(self) -> None:
        self.log(f"Setting up node '{self.name}'")
        self.log(f"Node '{self.name}' setup complete.")
    
    def record_new(self, filepath: str) -> Dict:
        self.metadata_store.create_entry(self.name, filepath=filepath, state="new")

    def record_current(self, filepath: str) -> None:
        self.metadata_store.create_entry(self.name, filepath=filepath, state="current", run_id=self.metadata_store.get_run_id())
    
    def start_monitoring(self) -> None:

        def _monitor_thread_func():
            self.log(f"Starting observer thread for node '{self.name}'")
            while self.exit_event.is_set() is False:
                for root, dirnames, filenames in os.walk(self.path):
                    for filename in filenames:
                        filepath = os.path.join(root, filename)
                        filepath = filepath.removeprefix(self.path)     # Remove the path prefix
                        filepath = filepath.lstrip(os.sep)              # Remove leading separator
                        if self.metadata_store.entry_exists(self, filepath) is False:
                            self.log(f"'{self.name}' detected file: {filepath}", level="INFO")
                            self.record_new(filepath)

                if self.exit_event.is_set() is True: break
                try:
                    self.custom_trigger()
                
                except NotImplementedError:
                    self.base_trigger()

                except Exception as e:
                        self.log(f"Error checking resource in node '{self.name}': {traceback.format_exc()}")
                        # Note: we continue here because we want to keep trying to check the resource until it is available
                        # with that said, we should add an option for the user to specify the number of times to try before giving up
                        # and throwing an exception
                        # Note: we also continue because we don't want to stop checking in the case of a corrupted file or something like that. 
                        # We should also think about adding an option for the user to specify what actions to take in the case of an exception,
                        # e.g., send an email to the data science team to let everyone know the resource is corrupted, 
                        # or just not move the file to current.
                
                # sleep for a while before checking again
                time.sleep(0.1)

        self.observer_thread = Thread(name=f"{self.name}_observer", target=_monitor_thread_func)
        self.observer_thread.start()

    def custom_trigger(self) -> bool:
        """
        Override to implement your custom triggering logic. If the custom_trigger method is not implemented, the base_trigger method will be called.
        """
        raise NotImplementedError
    
    def base_trigger(self) -> None:
        """
        The default trigger for the FilesystemStoreNode. 
        base_trigger checks if there are any new files in the resource directory and triggers the node if there are.
        base_trigger is called when the custom_trigger method is not implemented.
        """

        if self.get_num_artifacts("new") > 0:
            self.trigger(message=f"New files detected in {self.resource_path}.")
    
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

    def save_artifact(self, filepath: str, *args, **kwargs):
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
            # self.record_current(filepath)

            self._save_artifact_hook(artifact_save_path, *args, **kwargs)
            self.record_current(filepath)
            self.log(f"Saved artifact to {artifact_save_path}", level="INFO")

        except Exception as e:
            self.log(f"Failed to save artifact '{filepath}': {e}", level="ERROR")
            raise e

    @BaseResourceNode.log_exception
    def list_artifacts(self, state: str) -> List[Any]:
        entries = self.metadata_store.get_entries(self.name, state)
        artifacts = [os.path.join(self.path, entry["location"]) for entry in entries]
        return artifacts
    
    @BaseResourceNode.log_exception
    def get_artifact(self, id: int) -> Dict:
        """
        Get artifact entry by ID.

        Args:
            id: The ID of the artifact to retrieve
            
        Returns:
            Dict: The artifact entry
            
        Raises:
            EntryNotFoundError: If no entry exists with the specified ID
        """

        entries = self.metadata_store.get_entries(resource_node_name=self.name)
        for entry in entries:
            if entry["id"] == id:
                return entry
        raise EntryNotFoundError(f"No entry found with id: {id}")
    
    @BaseResourceNode.log_exception
    def get_num_artifacts(self, state: str) -> int:
        return self.metadata_store.get_num_entries(self.name, state)
    
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