import numpy as np
import sys
import os
from datetime import datetime
import json

sys.path.append("../../anacostia_pipeline")
from engine.node import ResourceNode

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


class FeatureStoreNode(ResourceNode, FileSystemEventHandler):
    def __init__(
        self, name: str, 
        path: str, 
        max_old_vectors: int = None, 
    ) -> None:

        self.max_old_vectors = max_old_vectors
        self.feature_store_path = os.path.join(os.path.abspath(path), "feature_store")
        self.feature_store_json_path = os.path.join(self.feature_store_path, "feature_store.json")
        self.observer = Observer()
        super().__init__(name, "feature_store")
    
    @ResourceNode.resource_accessor
    def setup(self) -> None:
        if os.path.exists(self.feature_store_path) is False:
            os.makedirs(self.feature_store_path, exist_ok=True)

        if os.path.exists(self.feature_store_json_path) is False:
            with open(self.feature_store_json_path, 'w') as json_file:
                json_entry = {
                    "node": self.name,
                    "resource_path": self.feature_store_path,
                    "files": []
                }

                for filepath in os.listdir(self.feature_store_path):
                    try:
                        if filepath.endswith(".json") is False:
                            path = os.path.join(self.feature_store_path, filepath)
                            array = np.load(path)
                            json_file_entry = {}
                            json_file_entry["filepath"] = os.path.join(path)
                            json_file_entry["num_samples"] = array.shape[0]
                            json_file_entry["shape"] = str(array.shape)
                            json_file_entry["state"] = "current"
                            json_file_entry["created_at"] = str(datetime.now())
                            json_entry["files"].append(json_file_entry)
                    except Exception as e:
                        self.log(f"Error loading feature vector file: {e}")
                        continue

                json.dump(json_entry, json_file, indent=4)
                self.log(f"Created feature_store.json file at {self.feature_store_json_path}")

        self.log(f"Setting up node '{self.name}'")
        self.observer.schedule(event_handler=self, path=self.feature_store_path, recursive=True)
        self.observer.start()
        self.log(f"Node '{self.name}' setup complete. Observer started, waiting for file change...")

    @ResourceNode.resource_accessor
    def on_modified(self, event):
        if not event.is_directory:
            with open(self.feature_store_json_path, 'r') as json_file:
                json_data = json.load(json_file)

            try:
                logged_files = [entry["filepath"] for entry in json_data["files"]]
                if (event.src_path.endswith(".json") is False) and (event.src_path not in logged_files):
                    array = np.load(os.path.join(self.feature_store_path, event.src_path))

                    json_entry = {}
                    json_entry["filepath"] = event.src_path
                    json_entry["num_samples"] = array.shape[0]
                    json_entry["shape"] = str(array.shape)
                    json_entry["state"] = "new"
                    json_entry["created_at"] = str(datetime.now())
                    json_data["files"].append(json_entry)

                    if self.trigger_condition() is True:
                        self.trigger()

            except Exception as e:
                self.log(f"Error processing {event.src_path}: {e}")
            
            with open(self.feature_store_json_path, 'w') as json_file:
                json.dump(json_data, json_file, indent=4)

    @ResourceNode.exeternally_accessible
    @ResourceNode.resource_accessor
    def create_filename(self) -> str:
        """
        Default implementaion to create a filename for the new feature vector file.
        Method can be overridden to create a custom filename; but user must ensure that the filename is unique.
        """
        num_files = len(os.listdir(self.feature_store_path))
        return f"features_{num_files}.npy"

    @ResourceNode.exeternally_accessible
    @ResourceNode.resource_accessor
    def save_feature_vector(self, feature_vector: np.ndarray) -> None:
        try:
            new_file_path = os.path.join(self.feature_store_path, self.create_filename())
            np.save(new_file_path, feature_vector)

            self.log(f"New feature vector saved: {new_file_path}")
        except Exception as e:
            self.log(f"Error saving feature vector: {e}")

    @ResourceNode.exeternally_accessible
    @ResourceNode.resource_accessor
    def get_feature_vectors(self, state: str) -> iter:
        if state not in ["current", "old", "new", "all"]:
            raise ValueError("state must be one of ['current', 'old', 'new', 'all']") 
        
        with open(self.feature_store_json_path, 'r') as json_file:
            json_data = json.load(json_file)
            feature_vectors_paths = [file_entry["filepath"] for file_entry in json_data["files"] if file_entry["state"] == state]

        for path in feature_vectors_paths:
            try:
                array = np.load(path)
                self.log(f"extracting current data from {path}")
                for row in array:
                    yield row

            except Exception as e:
                self.log(f"Error loading feature vector file: {e}")
                continue

    @ResourceNode.exeternally_accessible
    @ResourceNode.resource_accessor
    def get_feature_vectors_filepaths(self, state: str) -> iter:
        if state not in ["current", "old", "new", "all"]:
            raise ValueError("state must be one of ['current', 'old', 'new', 'all']") 

        with open(self.feature_store_json_path, 'r') as json_file:
            json_data = json.load(json_file)
            feature_vectors_paths = [file_entry["filepath"] for file_entry in json_data["files"] if file_entry["state"] == state]

        for path in feature_vectors_paths:
            yield path

    @ResourceNode.exeternally_accessible
    @ResourceNode.resource_accessor
    def trigger_condition(self) -> bool:
        # in the default implementation, we trigger the next node as soon as we see a new feature vectors file.
        return True

    @ResourceNode.exeternally_accessible
    @ResourceNode.resource_accessor
    def get_num_feature_vectors(self, state: str) -> int:
        if state not in ["current", "old", "new", "all"]:
            raise ValueError("state must be one of ['current', 'old', 'new', 'all']")
        
        with open(self.feature_store_json_path, 'r') as json_file:
            json_data = json.load(json_file)
            if state == "all":
                total_num_samples = sum([file_entry["num_samples"] for file_entry in json_data["files"]])
                return total_num_samples
            else:
                total_num_samples = sum([file_entry["num_samples"] for file_entry in json_data["files"] if file_entry["state"] == state])
                return total_num_samples

    @ResourceNode.await_references
    @ResourceNode.resource_accessor
    def execute(self):
        with open(self.feature_store_json_path, 'r') as json_file:
            json_data = json.load(json_file)

        for file_entry in json_data["files"]:
            if file_entry["state"] == "current":
                self.log(f'current -> old ({file_entry["num_samples"]} samples): {file_entry["filepath"]}')
                file_entry["state"] = "old"
        
        for file_entry in json_data["files"]:
            if file_entry["state"] == "new":
                self.log(f'new -> current ({file_entry["num_samples"]} samples): {file_entry["filepath"]}')
                file_entry["state"] = "current"

        with open(self.feature_store_json_path, 'w') as json_file:
            json.dump(json_data, json_file, indent=4)
    
        # max_old_vectors may be used to limit the number of feature vectors
        # stored in the feature store. If None, then there is no limit.
        # If the number of feature vectors exceeds the limit, then the oldest feature vectors will be deleted.
        # TODO: implement deletion of old feature vectors. implement the removal of old vectors in this method when state is updated.
        return True

    def on_exit(self) -> None:
        self.log(f"Beginning teardown for node '{self.name}'")
        self.observer.stop()
        self.observer.join()
        self.log(f"Observer stopped for node '{self.name}'")
        self.log(f"Node '{self.name}' exited")