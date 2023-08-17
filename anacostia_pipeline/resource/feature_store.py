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

        # max_old_vectors may be used to limit the number of feature vectors
        # stored in the feature store. If None, then there is no limit.
        # If the number of feature vectors exceeds the limit, then the oldest feature vectors will be deleted.
        self.max_old_vectors = max_old_vectors

        self.feature_store_path = os.path.join(path, "feature_store")
        self.feature_store_json_path = os.path.join(self.feature_store_path, "feature_store.json")
        self.observer = Observer()
        super().__init__(name, "feature_store")
    
    def setup(self) -> None:
        with self.resource_lock:
            if os.path.exists(self.feature_store_json_path) is False:
                os.makedirs(self.feature_store_path, exist_ok=True)

            if os.path.exists(self.feature_store_json_path) is False:
                with open(self.feature_store_json_path, 'w') as json_file:
                    json_entry = {
                        "node": self.name,
                        "files": []
                    }
                    json.dump(json_entry, json_file, indent=4)
        
            self.log(f"Setting up node '{self.name}'")
            self.observer.schedule(event_handler=self, path=self.feature_store_path, recursive=True)
            self.observer.start()
            self.log(f"Node '{self.name}' setup complete. Observer started, waiting for file change...")

    def get_num_current_feature_vectors(self) -> int:
        with self.current_resource_semaphore:
            with open(self.feature_store_json_path, 'r') as json_file:
                json_data = json.load(json_file)
                total_num_samples = sum([file_entry["num_samples"] for file_entry in json_data["files"] if file_entry["state"] == "current"])
                return total_num_samples

    def get_current_feature_vectors(self) -> list:
        # TODO: account for the case where the feature store is empty
        # TODO: account for the case where the feature store is not empty, but there are no current feature vectors
        # TODO: acquire resource semaphore, then release it after the method is done
        # once resource semaphore is back to 0, update the state of the feature vectors to "old"
        with self.current_resource_semaphore:
            with open(self.feature_store_json_path, 'r') as json_file:
                json_data = json.load(json_file)
                current_feature_vectors_paths = [file_entry["filepath"] for file_entry in json_data["files"] if file_entry["state"] == "current"]

        for path in current_feature_vectors_paths:
            # print(path)
            with self.current_resource_semaphore:
                try:
                    array = np.load(path)
                    for row in array:
                        yield row

                except Exception as e:
                    self.log(f"Error loading feature vector file: {e}")
                    continue
        
    def create_file_entry(self, filepath: str, num_samples: int, shape: tuple, state: str) -> dict:
        return {
            "filepath": filepath,
            "num_samples": num_samples,
            "shape": str(shape),
            "state": state,
            "created_at": str(datetime.now())
        }

    def on_modified(self, event):
        if not event.is_directory:
            with self.resource_lock:
                with open(self.feature_store_json_path, 'r') as json_file:
                    json_data = json.load(json_file)

                try:
                    # change of direction: use the on_modified method to monitor the change of the feature store json file
                    # once we see enough feature vectors, we can trigger the next node

                    if event.src_path.endswith(".npy"):
                        array = np.load(os.path.join(self.feature_store_path, event.src_path))

                        json_entry = {}
                        json_entry["filepath"] = event.src_path
                        json_entry["num_samples"] = array.shape[0]
                        json_entry["shape"] = str(array.shape)
                        json_entry["state"] = "new"
                        json_entry["created_at"] = str(datetime.now())
                        json_data["files"].append(json_entry)

                        self.log(f"New feature vectors detected: {event.event_type} {event.src_path}")

                except Exception as e:
                    self.log(f"Error processing {event.src_path}: {e}")
                
                with open(self.feature_store_json_path, 'w') as json_file:
                    json.dump(json_data, json_file, indent=4)

                # make sure signal is created before triggering
                self.trigger()

    def create_filename(self) -> str:
        """
        Default implementaion to create a filename for the new feature vector file.
        Method can be overridden to create a custom filename; but user must ensure that the filename is unique.
        """
        num_files = len(os.listdir(self.feature_store_path))
        return f"features_{num_files}.npy"

    def save_feature_vector(self, feature_vector: np.ndarray) -> None:
        with self.resource_lock:
            try:
                new_file_path = os.path.join(self.feature_store_path, self.create_filename())
                np.save(new_file_path, feature_vector)

                self.log(f"New feature vector saved: {new_file_path}")
            except Exception as e:
                self.log(f"Error saving feature vector: {e}")

    def update_state(self):
        with self.current_resource_semaphore:
            with self.resource_lock:
                with open(self.feature_store_json_path, 'r') as json_file:
                    json_data = json.load(json_file)

                for file_entry in json_data["files"]:
                    if file_entry["state"] == "current":
                        self.log(f'current -> old: {file_entry["filepath"]}')
                        file_entry["state"] = "old"
                
                for file_entry in json_data["files"]:
                    if file_entry["state"] == "new":
                        self.log(f'new -> current: {file_entry["filepath"]}')
                        file_entry["state"] = "current"

                with open(self.feature_store_json_path, 'w') as json_file:
                    json.dump(json_data, json_file, indent=4)
    
    def on_exit(self) -> None:
        self.log(f"Beginning teardown for node '{self.name}'")
        self.observer.stop()
        self.observer.join()
        self.log(f"Observer stopped for node '{self.name}'")