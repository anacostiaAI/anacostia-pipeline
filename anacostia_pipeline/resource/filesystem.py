import os
import time
import sys
sys.path.append("../../anacostia_pipeline")

from typing import Callable, List, Dict, Any
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileModifiedEvent

from engine.node import Node


class FileChangeHandler(FileSystemEventHandler):
    def __init__(self, node: Node) -> None:
        self.node = node

    # on modified event, send signal to parent nodes
    # DirModifiedEvent or FileModifiedEvent is triggered because we are watching an existing directory 
    # (i.e., the path argument in observer.schedule),
    # if we were creating the directory, we would have to watch for DirCreatedEvent
    def on_modified(self, event):
        if event.is_directory:
            print(f"Detected change: {event.event_type} {event.src_path}")
            self.node.send_signal()


class FileWatchNode(Node):
    def __init__(self, name, path):
        self.path = path
        super().__init__(name, "resource")
        self.handler = FileChangeHandler(self)
        self.observer = Observer()
    
    def start_file_watch(self):
        self.observer.schedule(self.handler, path=self.path, recursive=False)
        self.observer.start()

        # although we technically do have a while loop here,
        # the while loop here is running inside the while loop in the node.__listen function,
        # and because the while loop in the node.__listen function is running in a separate thread, 
        # the while loop here is not blocking
        while True:
            time.sleep(1)

    def trigger(self) -> bool:
        return self.start_file_watch()

    def teardown(self) -> None:
        self.observer.stop()
        self.observer.join()

if __name__ == "__main__":
    folder1_node = FileWatchNode("folder1", "/Users/minhquando/Desktop/anacostia/anacostia_pipeline/resource/folder1")
    folder1_node.start()

    time.sleep(30)