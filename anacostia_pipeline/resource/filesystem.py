import time
import sys
from logging import Logger
sys.path.append("../../anacostia_pipeline")

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from engine.node import ResourceNode


class DirWatchNode(ResourceNode, FileSystemEventHandler):
    def __init__(self, name: str, path: str, logger: Logger=None):
        self.path = path
        super().__init__(name, logger)
        self.observer = Observer()
    
    def on_modified(self, event):
        if event.is_directory:
            if self.logger is not None:
                self.logger.info(f"Detected change: {event.event_type} {event.src_path}")
            else:
                print(f"Detected change: {event.event_type} {event.src_path}")
            self.trigger()
    
    def setup(self) -> None:
        if self.logger is not None:
            self.logger.info(f"Setting up node '{self.name}'")
        else:
            print(f"Setting up node '{self.name}'")
        
        self.observer.schedule(event_handler=self, path=self.path, recursive=False)
        self.observer.start()

        if self.logger is not None:
            self.logger.info(f"Node '{self.name}' setup complete. Observer started, waiting for file change...")
        else:
            print(f"Node '{self.name}' setup complete. Observer started, waiting for file change...")
    
    def teardown(self) -> None:
        self.observer.stop()
        self.observer.join()
        if self.logger is not None:
            self.logger.info(f"Node '{self.name}' teardown complete.")
        else:
            print(f"Node '{self.name}' teardown complete.")


if __name__ == "__main__":
    folder1_node = DirWatchNode("folder1", "/Users/minhquando/Desktop/anacostia/anacostia_pipeline/resource/folder1")
    folder1_node.start()

    time.sleep(20)