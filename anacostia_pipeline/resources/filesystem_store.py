import os
from typing import List, Any, Union, Dict
from datetime import datetime
from logging import Logger
from threading import Thread

from ..engine.base import BaseMetadataStoreNode, BaseResourceNode
from ..dashboard.subapps.filesystemstore import FilesystemStoreNodeApp
from ..engine.constants import Status



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

    @BaseResourceNode.resource_accessor
    def setup(self) -> None:
        self.log(f"Setting up node '{self.name}'")
        self.metadata_store.create_resource_tracker(self)
        self.log(f"Node '{self.name}' setup complete.")
    
    @BaseResourceNode.resource_accessor
    def record_new(self, filepath: str) -> Dict:
        self.metadata_store.create_entry(self, filepath=filepath, state="new")

    @BaseResourceNode.resource_accessor
    def record_current(self, filepath: str) -> None:
        self.metadata_store.create_entry(self, filepath=filepath, state="current", run_id=self.metadata_store.get_run_id())
    
    def start_monitoring(self) -> None:

        def _monitor_thread_func():
            self.log(f"Starting observer thread for node '{self.name}'")
            while self.status == Status.RUNNING:
                with self.resource_lock:
                    for filename in os.listdir(self.path):
                        filepath = os.path.join(self.path, filename)
                        if self.metadata_store.entry_exists(self, filepath) is False:
                            self.log(f"'{self.name}' detected file: {filepath}")
                            self.record_new(filepath)
                
        self.observer_thread = Thread(name=f"{self.name}_observer", target=_monitor_thread_func)
        self.observer_thread.start()

    @BaseResourceNode.resource_accessor
    def trigger_condition(self) -> bool:
        # implement the triggering logic here
        return True
    
    @BaseResourceNode.log_exception
    @BaseResourceNode.resource_accessor
    def create_filename(self) -> str:
        return f"file{self.get_num_artifacts('all')}.txt"
    
    @BaseResourceNode.log_exception
    @BaseResourceNode.resource_accessor
    def save_artifact(self, content: str) -> None:
        pass

    @BaseResourceNode.log_exception
    @BaseResourceNode.resource_accessor
    def list_artifacts(self, state: str) -> List[Any]:
        entries = self.metadata_store.get_entries(self, state)
        artifacts = [entry["location"] for entry in entries]
        return artifacts
    
    @BaseResourceNode.log_exception
    def get_artifact(self, id: int) -> Dict:
        return self.metadata_store.get_entry(self, id)
    
    @BaseResourceNode.log_exception
    @BaseResourceNode.resource_accessor
    def get_num_artifacts(self, state: str) -> int:
        return self.metadata_store.get_num_entries(self, state)
    
    @BaseResourceNode.log_exception
    @BaseResourceNode.resource_accessor
    def load_artifact(self, artifact_path: str) -> Any:
        with open(artifact_path, "r") as f:
            content = f.read()
            return content

    def stop_monitoring(self) -> None:
        self.log(f"Beginning teardown for node '{self.name}'")
        self.observer_thread.join()
        self.log(f"Observer stopped for node '{self.name}'")