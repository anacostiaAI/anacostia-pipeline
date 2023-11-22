import sys
import os
from typing import List, Any, Union
from datetime import datetime
from logging import Logger

sys.path.append("../../anacostia_pipeline")
from engine.base import BaseMetadataStoreNode, BaseResourceNode

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler



class ArtifactStoreNode(BaseResourceNode, FileSystemEventHandler):
    def __init__(
        self, name: str, resource_path: str, metadata_store: BaseMetadataStoreNode, 
        init_state: str = "new", max_old_samples: int = None, loggers: Union[Logger, List[Logger]] = None, monitoring: bool = True
    ) -> None:

        # TODO: add max_old_samples functionality
        self.max_old_samples = max_old_samples
        
        self.path = os.path.abspath(resource_path)
        if os.path.exists(self.path) is False:
            os.makedirs(self.path, exist_ok=True)
        
        self.observer = Observer()
        
        if init_state not in ("new", "old"):
            raise ValueError(f"init_state argument of DataStoreNode must be either 'new' or 'old', not '{init_state}'.")
        self.init_state = init_state
        self.init_time = str(datetime.now())
        
        super().__init__(name=name, resource_path=resource_path, metadata_store=metadata_store, loggers=loggers, monitoring=monitoring)
    
    @BaseResourceNode.resource_accessor
    def setup(self) -> None:
        self.log(f"Setting up node '{self.name}'")
        self.metadata_store.create_resource_tracker(self)
        self.log(f"Node '{self.name}' setup complete.")
    
    def start_monitoring(self) -> None:
        self.observer.schedule(event_handler=self, path=self.path, recursive=True)
        self.observer.start()
        self.log(f"Observer started for node '{self.name}' monitoring path '{self.path}'")
    
    @BaseResourceNode.resource_accessor
    def record_new(self, filepath: str) -> None:
        self.metadata_store.create_entry(self, filepath=filepath, state="new")

    @BaseResourceNode.resource_accessor
    def record_current(self, filepath: str) -> None:
        self.metadata_store.create_entry(self, filepath=filepath, state="current", run_id=self.metadata_store.get_run_id())

    @BaseResourceNode.resource_accessor
    def on_modified(self, event):
        if not event.is_directory:
            self.log(f"'{self.name}' detected file: {event.src_path}")
            self.record_new(event.src_path) 

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
        artifacts = [entry["filepath"] for entry in entries]
        return artifacts
    
    @BaseResourceNode.log_exception
    @BaseResourceNode.resource_accessor
    def get_num_artifacts(self, state: str) -> int:
        return self.metadata_store.get_num_entries(self, state)
    
    def stop_monitoring(self) -> None:
        self.log(f"Beginning teardown for node '{self.name}'")
        self.observer.stop()
        self.observer.join()
        self.log(f"Observer stopped for node '{self.name}'")