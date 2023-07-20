import os
import time
import sys
sys.path.append("../../anacostia_pipeline")

from typing import Callable, List, Dict, Any
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from engine.node import Node


# Function to retrieve the list of files in a directory
def get_directory_files(path):
    return [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]

class FilesystemNode(Node):
    def __init__(self, name, path):
        self.path = path
        super().__init__(name)
    
    def on_create(path):
        # Get the initial list of files in the directory
        files = get_directory_files(path)

        try:
            while True:
                # Check for new files
                new_files = get_directory_files(path)
                added_files = set(new_files) - set(files)

                if added_files:
                    for file in added_files:
                        print(f"New file added: {os.path.join(path, file)}")

                    # send signal to the node
                    return True

                # Update the list of files
                files = new_files

                # Sleep for a specified interval
                time.sleep(1)

        except KeyboardInterrupt:
            pass
    
    def trigger(self) -> bool:
        return self.on_create(self.path)


class FileChangeHandler(FileSystemEventHandler):
    def __init__(self, node: Node) -> None:
        self.node = node

    def on_created(self, event):
        if event.is_directory:
            print("New file added:", event.src_path)
            self.node.send_signal()

    def on_modified(self, event):
        if event.is_directory:
            print("File modified:", event.src_path)
            self.node.send_signal()

    def on_deleted(self, event):
        if event.is_directory:
            print("File deleted:", event.src_path)
            self.node.send_signal()

class FileWatchNode(Node):
    def __init__(self, name, path):
        self.path = path
        self.handler = FileChangeHandler(self)
        super().__init__(name)
    
    def start_file_watch(self, directory):
        event_handler = self.handler
        observer = Observer()
        observer.schedule(event_handler, path=directory, recursive=False)
        observer.start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
        observer.join()

    def trigger(self) -> bool:
        return self.start_file_watch(self.path)


if __name__ == "__main__":
    #folder1_node = FilesystemNode("folder1", "/Users/minhquando/Desktop/anacostia/anacostia_pipeline/resource/folder1")
    #folder1_node.setup()
    folder1_node = FileWatchNode("folder1", "/Users/minhquando/Desktop/anacostia/anacostia_pipeline/resource/folder1")
    folder1_node.setup()

    time.sleep(30)