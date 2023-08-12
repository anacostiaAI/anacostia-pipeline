import sys
import os
import time
from typing import Dict, List
sys.path.append("../../anacostia_pipeline")

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from engine.node import ResourceNode
from engine.dag import DAG


class ModelRegistryNode(ResourceNode, FileSystemEventHandler):
    def __init__(self, name: str, path: str) -> None:
        self.model_resgistry_path = os.path.join(path, "model_registry")
        self.old_models_path = os.path.join(self.model_resgistry_path, "old_models")
        self.new_models_path = os.path.join(self.model_resgistry_path, "new_models")
        
        self.last_checked_time = time.time()
        
        self.observer = Observer()
        super().__init__(name, "model_registry")

        self.signal = super().signal_message_template()
    
    def setup(self) -> None:
        os.makedirs(self.model_resgistry_path, exist_ok=True)
        os.makedirs(self.old_models_path, exist_ok=True)
        os.makedirs(self.new_models_path, exist_ok=True)

        if self.logger is not None:
            self.logger.info(f"Setting up node '{self.name}'")
        else:
            print(f"Setting up node '{self.name}'")
        
        self.observer.schedule(event_handler=self, path=self.new_models_path, recursive=True)
        self.observer.start()

        if self.logger is not None:
            self.logger.info(f"Node '{self.name}' setup complete. Observer started, waiting for file change...")
        else:
            print(f"Node '{self.name}' setup complete. Observer started, waiting for file change...")

    def signal_message_template(self) -> Dict[str, List[str]]:
        return self.signal

    def on_modified(self, event):
        with self.get_resource_lock():
            if event.is_directory:
                if self.logger is not None:
                    for model_paths in self.new_models_paths():
                        self.logger.info(f"New models detected: {event.event_type} {model_paths}")
                else:
                    for model_paths in self.new_models_paths():
                        print(f"New models detected: {event.event_type} {model_paths}")

                # make sure signal is created before triggering
                self.signal["added_files"] = self.new_models_paths()
                self.trigger()

            self.last_checked_time = time.time()

    def load_old_models(self, limit: int = 1) -> None:
        print("loading old models")
        time.sleep(1)
        print("old models loaded")

    def new_models_paths(self) -> None:
        models = os.listdir(self.new_models_path)
        return models
    
    def save_model(self, model_name: str) -> None:
        print(f"saving model {model_name} to registry {self.model_resgistry_path}")
        time.sleep(1)
        print(f"model {model_name} saved")

    def teardown(self) -> None:
        self.observer.stop()
        self.observer.join()
        if self.logger is not None:
            self.logger.info(f"Node '{self.name}' teardown complete.")
        else:
            print(f"Node '{self.name}' teardown complete.")


if __name__ == "__main__":
    model_registry_node = ModelRegistryNode(name="model_registry", path="../../tests/testing_artifacts")
    DAG().start()
