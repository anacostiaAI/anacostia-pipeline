import sys
import os
import json
from typing import List, Any
from datetime import datetime
import time

sys.path.append("../../anacostia_pipeline")
from engine.base import BaseResourceNode
from engine.constants import Status, Work, Result

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler



class ArtifactStoreNode(BaseResourceNode, FileSystemEventHandler):
    def __init__(self, name: str, path: str, init_state: str = "current", max_old_samples: int = None) -> None:
        self.max_old_samples = max_old_samples
        self.data_store_path = os.path.abspath(path)
        self.data_store_json_path = os.path.join(self.data_store_path, "data_store.json")
        self.observer = Observer()
        
        if init_state not in ["current", "old"]:
            raise ValueError(f"init_state argument of DataStoreNode must be either 'current' or 'old', not '{init_state}'.")
        self.init_state = init_state
        self.init_time = str(datetime.now())
        
        super().__init__(name, "data_store")
    
    @BaseResourceNode.resource_accessor
    def setup(self) -> None:
        self.log(f"Setting up node '{self.name}'")

        if os.path.exists(self.data_store_path) is False:
            os.makedirs(self.data_store_path, exist_ok=True)
        
        if os.path.exists(self.data_store_json_path) is False:
            with open(self.data_store_json_path, "w") as json_file:
                json_entry = {
                    "node": self.name,
                    "resource path": self.data_store_path,
                    "node initialization time:": self.init_time,
                    "files": []
                }
                
                for filepath in os.listdir(self.data_store_path):
                    if filepath.endswith(".json") is False:
                        path = os.path.join(self.data_store_path, filepath)
                        json_file_entry = {}
                        json_file_entry["filepath"] = os.path.join(path)
                        json_file_entry["state"] = self.init_state
                        json_file_entry["created_at"] = self.init_time
                        json_entry["files"].append(json_file_entry)
                        self.log(f"Data store is not empty at initialization, adding to data_store.json: {filepath}")
                
                json.dump(json_entry, json_file, indent=4)
                self.log(f"Created data_store.json file at {self.data_store_json_path}")

        self.observer.schedule(event_handler=self, path=self.data_store_path, recursive=True)
        self.observer.start()
        self.log(f"Node '{self.name}' setup complete. Observer started, waiting for file change...")

        # if the directory is not empty at initialization and there are files marked as "current", immediately signal successor node
        # Note: this means that the setup method does not obey the user's trigger condition criteria 
        # because we can't call trigger_condition() in this setup functon because the trigger_condition() method uses the 
        # @ResourceNode.resource_accessor decorator which prevents the trigger_condition() method from running until setup() finishes executing
        with open(self.data_store_json_path, 'r') as json_file:
            json_data = json.load(json_file)
            filepaths = [entry["filepath"] for entry in json_data["files"] if entry["state"] == "current"]

            """
            if len(filepaths) > 0:
                self.log("signaling next node on setup")
                self.send_signals(Status.SUCCESS) 
            """
    
    @BaseResourceNode.resource_accessor
    def on_modified(self, event):
        if not event.is_directory:
            with open(self.data_store_json_path, 'r') as json_file:
                json_data = json.load(json_file)
            
            try:
                logged_files = [entry["filepath"] for entry in json_data["files"]]
                if (event.src_path.endswith(".json") is False) and (event.src_path not in logged_files):
                    json_entry = {}
                    json_entry["filepath"] = event.src_path
                    json_entry["state"] = "new"
                    json_entry["created_at"] = str(datetime.now())
                    json_data["files"].append(json_entry)

                    # check trigger condition here 

            except Exception as e:
                self.log(f"Error processing {event.src_path}: {e}")
            
            with open(self.data_store_json_path, 'w') as json_file:
                json.dump(json_data, json_file, indent=4)

    def update_state(self) -> None:
        """
        self.log(f"Updating state of node '{self.name}'")
        with open(self.data_store_json_path, 'r') as json_file:
            json_data = json.load(json_file)

        for file_entry in json_data["files"]:
            if file_entry["state"] == "current":
                self.log(f'{self.name} current -> old: {file_entry["filepath"]}')
                file_entry["state"] = "old"
        
        for file_entry in json_data["files"]:
            if file_entry["state"] == "new":
                self.log(f'{self.name} new -> current: {file_entry["filepath"]}')
                file_entry["state"] = "current"

        with open(self.data_store_json_path, 'w') as json_file:
            json.dump(json_data, json_file, indent=4)
        """
    
    def on_exit(self) -> None:
        self.log(f"Beginning teardown for node '{self.name}'")
        self.observer.stop()
        self.observer.join()
        self.log(f"Observer stopped for node '{self.name}'")
        self.log(f"Node '{self.name}' exited")