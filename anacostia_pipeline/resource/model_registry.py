import sys
import os
import json
from datetime import datetime

sys.path.append("../../anacostia_pipeline")
from engine.node import ResourceNode
from engine.constants import Status

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


class ModelRegistryNode(ResourceNode, FileSystemEventHandler):
    def __init__(self, name: str, path: str, framework: str, init_state: str = "current", max_old_models: int = None) -> None:
        self.model_registry_path = os.path.join(os.path.abspath(path), "model_registry")
        self.model_registry_json_path = os.path.join(self.model_registry_path, "model_registry.json")
        self.framework = framework
        self.max_old_models = max_old_models
        self.init_state = init_state
        self.observer = Observer()
        super().__init__(name, "model_registry")

    @ResourceNode.resource_accessor
    def setup(self) -> None:
        if os.path.exists(self.model_registry_path) is False:
            os.makedirs(self.model_registry_path, exist_ok=True)

        if os.path.exists(self.model_registry_json_path) is False:
            with open(self.model_registry_json_path, 'w') as json_file:
                json_entry = {
                    "node": self.name,
                    "resource_path": self.model_registry_path,
                    "models": []
                }

                for filepath in os.listdir(self.model_registry_path):
                    if filepath.endswith(".json") is False:
                        path = os.path.join(self.model_registry_path, filepath)
                        json_file_entry = {}
                        json_file_entry["model_path"] = os.path.join(path)
                        json_file_entry["framework"] = self.framework
                        json_file_entry["state"] = self.init_state
                        json_file_entry["created_at"] = str(datetime.now())
                        json_entry["models"].append(json_file_entry)

                json.dump(json_entry, json_file, indent=4)

        self.log(f"Setting up node '{self.name}'")
        self.observer.schedule(event_handler=self, path=self.model_registry_path, recursive=True)
        self.observer.start()
        self.log(f"Node '{self.name}' setup complete. Observer started, waiting for file change...")

        with open(self.model_registry_json_path, 'r') as json_file:
            json_data = json.load(json_file)
            filepaths = [entry["model_path"] for entry in json_data["models"] if entry["state"] == "current"]

            if len(filepaths) > 0:
                self.log("signaling next node on setup")
                self.send_signals(Status.SUCCESS) 

    @ResourceNode.resource_accessor
    def on_modified(self, event):
        if not event.is_directory:
            with open(self.model_registry_json_path, 'r') as json_file:
                json_data = json.load(json_file)
            
            try:
                logged_files = [entry["model_path"] for entry in json_data["models"]]
                if (event.src_path.endswith(".json") is False) and (event.src_path not in logged_files):
                    self.log(f"New model detected: {event.src_path}")
                    json_entry = {}
                    json_entry["model_path"] = event.src_path
                    json_entry["framework"] = self.framework
                    json_entry["state"] = "new"
                    json_entry["created_at"] = str(datetime.now())
                    json_data["models"].append(json_entry)

                    if self.trigger_condition() is True:
                        self.trigger()

            except Exception as e:
                self.log(f"Error processing {event.src_path}: {e}")
            
            with open(self.model_registry_json_path, 'w') as json_file:
                json.dump(json_data, json_file, indent=4)

    @ResourceNode.exeternally_accessible
    @ResourceNode.resource_accessor
    def trigger_condition(self) -> bool:
        # in the default implementation, we trigger the next node as soon as we see a new model in the registry.
        return True

    @ResourceNode.exeternally_accessible
    @ResourceNode.resource_accessor
    def create_filename(self, file_extension: str = None) -> str:
        """
        Default implementaion to create a filename for the new feature vector file.
        Method can be overridden to create a custom filename; but user must ensure that the filename is unique.
        """

        if file_extension is None:
            if self.framework == "tensorflow":
                file_extension = "h5"
            elif self.framework == "pytorch":
                file_extension = "pth"
            elif self.framework == "sklearn":
                file_extension = "pkl"
            elif self.framework == "onnx":
                file_extension = "onnx"
            else:
                raise ValueError(
                    f"Framework '{self.framework}' not supported. If you want to use this framework, please specify a file extension."
                )

        num_files = len(os.listdir(self.model_registry_path))
        return f"model_{num_files}.{file_extension}"

    @ResourceNode.exeternally_accessible
    @ResourceNode.resource_accessor 
    def save_model(self) -> None:
        raise NotImplementedError

    @ResourceNode.exeternally_accessible
    @ResourceNode.resource_accessor
    def get_models_paths(self, state: str, return_immediately: bool = True) -> list[str]:
        if state not in ["current", "old", "new", "all"]:
            raise ValueError("state must be one of ['current', 'old', 'new', 'all']")
        
        with open(self.model_registry_json_path, 'r') as json_file:
            json_data = json.load(json_file)

        current_models = []
        for file_entry in json_data["models"]:
            if state == "all":
                current_models.append(file_entry["model_path"])
            else:
                if file_entry["state"] == state:
                    current_models.append(file_entry["model_path"])

        return current_models
    
    @ResourceNode.exeternally_accessible
    @ResourceNode.resource_accessor
    def load_model(self) -> None:
        raise NotImplementedError

    @ResourceNode.await_references
    @ResourceNode.resource_accessor
    def execute(self):
        with open(self.model_registry_json_path, 'r') as json_file:
            json_data = json.load(json_file)

        for file_entry in json_data["models"]:
            if file_entry["state"] == "current":
                self.log(f'current -> old: {file_entry["model_path"]}')
                file_entry["state"] = "old"
        
        for file_entry in json_data["models"]:
            if file_entry["state"] == "new":
                self.log(f'new -> current: {file_entry["model_path"]}')
                file_entry["state"] = "current"

        with open(self.model_registry_json_path, 'w') as json_file:
            json.dump(json_data, json_file, indent=4)
        
        return True
    
    def on_exit(self) -> None:
        self.log(f"Beginning teardown for node '{self.name}'")
        self.observer.stop()
        self.observer.join()
        self.log(f"Observer stopped for node '{self.name}'")
