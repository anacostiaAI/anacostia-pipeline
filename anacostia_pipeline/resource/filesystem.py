import os
import time
import sys
sys.path.append("../../anacostia_pipeline")

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from engine.node import ResourceNode


class DirChangeHandler(FileSystemEventHandler):
    def __init__(self, node: ResourceNode) -> None:
        self.node = node

    # on modified event, send signal to parent nodes
    # DirModifiedEvent or FileModifiedEvent is triggered because we are watching an existing directory 
    # (i.e., the path argument in observer.schedule),
    # if we were creating the directory, we would have to watch for DirCreatedEvent
    def on_modified(self, event):
        if event.is_directory:
            print(f"Detected change: {event.event_type} {event.src_path}")
            self.node.trigger()


class DirWatchNode(ResourceNode):
    def __init__(self, name, path):
        self.path = path
        self.name = name
        super().__init__(name, "resource")
        self.handler = DirChangeHandler(self)
        self.observer = Observer()
    
    def setup(self) -> None:
        print(f"Setting up node '{self.name}'")
        self.observer.schedule(self.handler, path=self.path, recursive=False)
        self.observer.start()
        print("Observer started, waiting for file change...")
        print(f"Node '{self.name}' setup complete")
    
    def teardown(self) -> None:
        self.observer.stop()
        self.observer.join()

if __name__ == "__main__":
    folder1_node = DirWatchNode("folder1", "/Users/minhquando/Desktop/anacostia/anacostia_pipeline/resource/folder1")
    folder1_node.start()

    time.sleep(20)