import sys
import os
import json
from datetime import datetime

sys.path.append("../../anacostia_pipeline")
from engine.node import ResourceNode

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


class DataStoreNode(ResourceNode, FileSystemEventHandler):
    def __init__(
        self, 
        name: str, 
        path: str,
        max_old_samples: int = None
    ) -> None:
        self.max_old_samples = max_old_samples
        self.data_store_path = path
        self.data_store_json_path = os.path.join(self.data_store_path, "data_store.json")
        self.observer = Observer()
        super().__init__(name, "data_store")
    
    @ResourceNode.lock_decorator
    def setup(self) -> None:
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

                json.dump(json_entry, json_file, indent=4)

        self.log(f"Setting up node '{self.name}'")
        self.observer.schedule(event_handler=self, path=self.data_store_path, recursive=True)
        self.observer.start()
        self.log(f"Node '{self.name}' setup complete. Observer started, waiting for file change...")

    @ResourceNode.lock_decorator
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

            except Exception as e:
                self.log(f"Error processing {event.src_path}: {e}")
            
            with open(self.data_store_json_path, 'w') as json_file:
                json.dump(json_data, json_file, indent=4)

            # make sure signal is created before triggering
            self.trigger()

    @ResourceNode.wait_successors
    @ResourceNode.lock_decorator
    def update_state(self):
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
    
    def on_exit(self) -> None:
        self.log(f"Beginning teardown for node '{self.name}'")
        self.observer.stop()
        self.observer.join()
        self.log(f"Observer stopped for node '{self.name}'")
