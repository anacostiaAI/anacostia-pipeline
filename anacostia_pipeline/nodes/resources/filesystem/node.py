import os
from typing import List, Any, Union, Dict
from datetime import datetime
from logging import Logger
from threading import Thread
import traceback
import fcntl
from contextlib import contextmanager

from anacostia_pipeline.nodes.resources.node import BaseResourceNode
from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.nodes.resources.filesystem.app import FilesystemStoreNodeApp



@contextmanager
def locked_file(filename, mode='r'):
    with open(filename, mode) as file:
        fcntl.flock(file.fileno(), fcntl.LOCK_SH if mode.startswith('r') else fcntl.LOCK_EX)
        try:
            yield file
        finally:
            fcntl.flock(file.fileno(), fcntl.LOCK_UN)

        # use a shared lock (fcntl.LOCK_SH) for reading:
        # - allows multiple processes to acquire a shared lock for reading
        # - multiple readers can access the file simultaneously
        # - prevents any process from acquiring an exclusive lock (fcntl.LOCK_EX) for writing while readers have the file open

        # use an exclusive lock (fcntl.LOCK_EX) for writing
        # - allows only one process to acquire an exclusive lock for writing
        # - prevents any other process from acquiring a shared or exclusive lock for reading or writing
        # - ensures that only one writer can modify the file at a time, and no readers can access it during the write operation



class FilesystemStoreNode(BaseResourceNode):
    def __init__(
        self, name: str, resource_path: str, metadata_store: BaseMetadataStoreNode, 
        init_state: str = "new", max_old_samples: int = None, loggers: Union[Logger, List[Logger]] = None, monitoring: bool = True
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
        
        super().__init__(name=name, resource_path=resource_path, metadata_store=metadata_store, loggers=loggers, monitoring=monitoring)
    
    def get_app(self) -> FilesystemStoreNodeApp:
        return FilesystemStoreNodeApp(self)

    def setup(self) -> None:
        self.log(f"Setting up node '{self.name}'")
        self.log(f"Node '{self.name}' setup complete.")
    
    def record_new(self, filepath: str) -> Dict:
        self.metadata_store.create_entry(self, filepath=filepath, state="new")

    def record_current(self, filepath: str) -> None:
        self.metadata_store.create_entry(self, filepath=filepath, state="current", run_id=self.metadata_store.get_run_id())
    
    def start_monitoring(self) -> None:

        def _monitor_thread_func():
            self.log(f"Starting observer thread for node '{self.name}'")
            while self.exit_event.is_set() is False:
                for filename in os.listdir(self.path):
                    filepath = os.path.join(self.path, filename)
                    if self.metadata_store.entry_exists(self, filepath) is False:
                        self.log(f"'{self.name}' detected file: {filepath}")
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
            self.trigger()
    
    @BaseResourceNode.log_exception
    def create_filename(self) -> str:
        return f"file{self.get_num_artifacts('all')}.txt"
    
    @BaseResourceNode.log_exception
    def save_artifact(self, content: str) -> None:
        pass

    @BaseResourceNode.log_exception
    def list_artifacts(self, state: str) -> List[Any]:
        entries = self.metadata_store.get_entries(self, state)
        artifacts = [entry["location"] for entry in entries]
        return artifacts
    
    @BaseResourceNode.log_exception
    def get_artifact(self, id: int) -> Dict | None:
        entries = self.metadata_store.get_entries(resource_node=self)
        for entry in entries:
            if entry["id"] == id:
                return entry
        return None
    
    @BaseResourceNode.log_exception
    def get_num_artifacts(self, state: str) -> int:
        return self.metadata_store.get_num_entries(self, state)
    
    @BaseResourceNode.log_exception
    def load_artifact(self, artifact_path: str) -> Any:
        with locked_file(artifact_path, "r") as file:
            return file.read()

    def stop_monitoring(self) -> None:
        self.log(f"Beginning teardown for node '{self.name}'")
        self.observer_thread.join()
        self.log(f"Observer stopped for node '{self.name}'")