import sys
import os
import time
from typing import Dict, List
sys.path.append("../../anacostia_pipeline")

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from engine.node import ResourceNode
from engine.pipeline import Pipeline


class ModelRegistryNode(ResourceNode, FileSystemEventHandler):
    def __init__(self, name: str, path: str, num_old_models: int = 1, num_new_models: int = 1) -> None:
        self.model_resgistry_path = os.path.join(path, "model_registry")
        self.old_models_path = os.path.join(self.model_resgistry_path, "old_models")
        self.new_models_path = os.path.join(self.model_resgistry_path, "new_models")
        self.num_old_models = num_old_models
        
        self.last_checked_time = time.time()
        
        self.observer = Observer()
        super().__init__(name, "model_registry")

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

    def on_modified(self, event):
        with self.resource_lock:
            if event.is_directory:
                if self.logger is not None:
                    for model_paths in self.new_models_paths():
                        self.logger.info(f"New models detected: {event.event_type} {model_paths}")
                else:
                    for model_paths in self.new_models_paths():
                        print(f"New models detected: {event.event_type} {model_paths}")

                # make sure signal is created before triggering
                self.trigger()

            self.last_checked_time = time.time()

    """
    def load_old_model(self, path: str) -> None:
        raise NotImplementedError
    
    def load_new_model(self, path: str) -> None:
        raise NotImplementedError

    def load_new_models(self) -> None:
        for model_path in self.new_models_paths():
            yield self.load_new_model(model_path)
    
    def load_old_models(self) -> None:
        for i, model_path in enumerate(self.old_models_paths()):
            if i < self.num_old_models:
                yield self.load_old_model(model_path)
    """

    def old_models_paths(self) -> None:
        models = os.listdir(self.old_models_path)
        return models

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
    dag = Pipeline([model_registry_node])
    dag.start()
