import sys
import os
import json
from typing import List, Any
from datetime import datetime
import time

sys.path.append("../../anacostia_pipeline")
from engine.node import ResourceNode

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


class DataStoreNode(ResourceNode, FileSystemEventHandler):
    def __init__(self, name: str, path: str, max_old_samples: int = None) -> None:
        self.max_old_samples = max_old_samples
        self.data_store_path = os.path.abspath(path)
        self.data_store_json_path = os.path.join(self.data_store_path, "data_store.json")
        self.observer = Observer()
        super().__init__(name, "data_store")
    
    @ResourceNode.resource_accessor
    def setup(self) -> None:
        self.log(f"Setting up node '{self.name}'")

        if os.path.exists(self.data_store_path) is False:
            os.makedirs(self.data_store_path, exist_ok=True)
        
        if os.path.exists(self.data_store_json_path) is False:
            with open(self.data_store_json_path, "w") as json_file:
                json_entry = {
                    "node": self.name,
                    "resource_path": self.data_store_path,
                    "files": []
                }
                
                for filepath in os.listdir(self.data_store_path):
                    if filepath.endswith(".json") is False:
                        path = os.path.join(self.data_store_path, filepath)
                        json_file_entry = {}
                        json_file_entry["filepath"] = os.path.join(path)
                        json_file_entry["state"] = "current"
                        json_file_entry["created_at"] = str(datetime.now())
                        json_entry["files"].append(json_file_entry)
                        self.log(f"Data store is not empty at initialization, adding to data_store.json: {filepath}")

                json.dump(json_entry, json_file, indent=4)
                self.log(f"Created data_store.json file at {self.data_store_json_path}")

        self.observer.schedule(event_handler=self, path=self.data_store_path, recursive=True)
        self.observer.start()
        self.log(f"Node '{self.name}' setup complete. Observer started, waiting for file change...")

    @ResourceNode.resource_accessor
    def on_modified(self, event):
        if not event.is_directory:
            with open(self.data_store_json_path, 'r') as json_file:
                json_data = json.load(json_file)
            
            try:
                # change of direction: use the on_modified method to monitor the change of the model_registry.json file
                # once we see a new model, we can trigger the next node
                logged_files = [entry["filepath"] for entry in json_data["files"]]
                if (event.src_path.endswith(".json") is False) and (event.src_path not in logged_files):
                    json_entry = {}
                    json_entry["filepath"] = event.src_path
                    json_entry["state"] = "new"
                    json_entry["created_at"] = str(datetime.now())
                    json_data["files"].append(json_entry)

                    if self.trigger_condition() is True:
                        self.trigger()

            except Exception as e:
                self.log(f"Error processing {event.src_path}: {e}")
            
            with open(self.data_store_json_path, 'w') as json_file:
                json.dump(json_data, json_file, indent=4)

    @ResourceNode.exeternally_accessible
    @ResourceNode.resource_accessor
    def create_filename(self, file_extension: str = None) -> str:
        num_files = len(os.listdir(self.data_store_path))
        return f"data_{num_files}.{file_extension}"

    @ResourceNode.exeternally_accessible
    @ResourceNode.resource_accessor
    def save_data_sample(self) -> None:
        raise NotImplementedError

    @ResourceNode.exeternally_accessible
    @ResourceNode.resource_accessor
    def load_data_sample(self, filepath: str) -> Any:
        raise NotImplementedError

    @ResourceNode.exeternally_accessible
    @ResourceNode.resource_accessor
    def trigger_condition(self) -> bool:
        # in the default implementation, we trigger the next node as soon as we see a new data file.
        return True
    
    @ResourceNode.exeternally_accessible
    @ResourceNode.resource_accessor
    def load_data_samples(self, state: str) -> iter:
        if state not in ["current", "old", "new", "all"]:
            raise ValueError("state must be one of ['current', 'old', 'new', 'all']")
        
        with open(self.data_store_json_path, 'r') as json_file:
            json_data = json.load(json_file)
            filepaths = [entry["filepath"] for entry in json_data["files"] if entry["state"] == state]
        
        for filepath in filepaths:
            yield self.load_data_sample(filepath)

    @ResourceNode.exeternally_accessible
    @ResourceNode.resource_accessor
    def load_data_paths(self, state: str) -> iter:
        if state not in ["current", "old", "new", "all"]:
            raise ValueError("state must be one of ['current', 'old', 'new', 'all']")
        
        with open(self.data_store_json_path, 'r') as json_file:
            json_data = json.load(json_file)
            filepaths = [entry["filepath"] for entry in json_data["files"] if entry["state"] == state]
        
        for filepath in filepaths:
            yield filepath

    @ResourceNode.exeternally_accessible
    @ResourceNode.resource_accessor
    def get_num_data_samples(self, state: str) -> int:
        if state not in ["current", "old", "new", "all"]:
            raise ValueError("state must be one of ['current', 'old', 'new', 'all']")
        
        with open(self.data_store_json_path, 'r') as json_file:
            json_data = json.load(json_file)
            filepaths = [entry["filepath"] for entry in json_data["files"] if entry["state"] == state]
        
        return len(filepaths)

    @ResourceNode.await_references
    @ResourceNode.resource_accessor
    def execute(self):
        self.log(f"Updating state of node '{self.name}'")
        with open(self.data_store_json_path, 'r') as json_file:
            json_data = json.load(json_file)

        for file_entry in json_data["files"]:
            if file_entry["state"] == "current":
                self.log(f'current -> old: {file_entry["filepath"]}')
                file_entry["state"] = "old"
        
        for file_entry in json_data["files"]:
            if file_entry["state"] == "new":
                self.log(f'new -> current: {file_entry["filepath"]}')
                file_entry["state"] = "current"

        with open(self.data_store_json_path, 'w') as json_file:
            json.dump(json_data, json_file, indent=4)
    
        return True

    def on_exit(self) -> None:
        self.log(f"Beginning teardown for node '{self.name}'")
        self.observer.stop()
        self.observer.join()
        self.log(f"Observer stopped for node '{self.name}'")
        self.log(f"Node '{self.name}' exited")
