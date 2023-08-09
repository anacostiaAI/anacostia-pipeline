import time
import sys
import os
from typing import Any, Dict, List
sys.path.append("../../anacostia_pipeline")

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from engine.node import ResourceNode
from engine.dag import DAG


observer = Observer()


def get_file_states(directory: str):
    file_states = {}
    for root, _, files in os.walk(directory):
        for filename in files:
            filepath = os.path.join(root, filename)
            file_states[filepath] = os.path.getmtime(filepath)
    return file_states


def get_new_files(directory: str, prev_states:  Dict[str, List[str]]) -> List[str]:
    current_states = get_file_states(directory)
    added_files = [filepath for filepath in current_states if filepath not in prev_states]
    return added_files


def get_modified_files(directory: str, prev_states:  Dict[str, List[str]]) -> List[str]:
    current_states = get_file_states(directory)
    modified_files = [filepath for filepath in current_states if prev_states.get(filepath) != current_states.get(filepath)]
    return modified_files


def get_removed_files(directory: str, prev_states:  Dict[str, List[str]]) -> List[str]:
    current_states = get_file_states(directory)
    removed_files = [filepath for filepath in prev_states if filepath not in current_states]
    return removed_files


class DirWatchNode(ResourceNode, FileSystemEventHandler):
    def __init__(self, name: str, path: str):
        self.path = path
        super().__init__(name, "folderwatch")
        self.directory_state = get_file_states(self.path)
    
    def signal_message_template(self) -> Dict[str, List[str]]:
        signal = super().signal_message_template()
        signal["added_files"] = get_new_files(self.path, self.directory_state)
        signal["modified_files"] = get_modified_files(self.path, self.directory_state)
        signal["removed_files"] = get_removed_files(self.path, self.directory_state)
        return signal
    
    def get_changed_files(self, prev_state: Dict[str, List[str]], difference: str = "modified") -> List[str]:
        with self.get_resource_lock():
            if difference == "added":
                changed_files = get_new_files(self.path, prev_state)
            elif difference == "modified":
                changed_files = get_modified_files(self.path, prev_state)
            elif difference == "removed":
                changed_files = get_removed_files(self.path, prev_state)
            
            self.directory_state = get_file_states(self.path)
            return changed_files

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
        
        observer.schedule(event_handler=self, path=self.path, recursive=True)
        observer.start()

        if self.logger is not None:
            self.logger.info(f"Node '{self.name}' setup complete. Observer started, waiting for file change...")
        else:
            print(f"Node '{self.name}' setup complete. Observer started, waiting for file change...")
    
    def teardown(self) -> None:
        observer.stop()
        observer.join()
        if self.logger is not None:
            self.logger.info(f"Node '{self.name}' teardown complete.")
        else:
            print(f"Node '{self.name}' teardown complete.")


class FileWatchNode(ResourceNode, FileSystemEventHandler):
    def __init__(self, name: str, path: str) -> None:
        self.path = path
        super().__init__(name, "filewatch")

        # the Observer class in watchdog uses a threading.RLock() to monitor the directory
        # this reentrant lock is not picklable, so we cannot use it in a multiprocessing environment
        #self.observer = Observer()

    def on_modified(self, event):
        if event.is_directory:
            print(f"Detected change: {event.event_type} {event.src_path}")
            self.trigger()
    
    def setup(self) -> None:
        print(f"Setting up node '{self.name}'")
        observer.schedule(event_handler=self, path=self.path, recursive=True)
        observer.start()
        print("setup complete")
    
    def teardown(self) -> None:
        observer.stop()
        observer.join()
        print("tearing down DirWatchNode node")
        time.sleep(1)
        print("teardown complete")


if __name__ == "__main__":
    folder1_node = DirWatchNode("folder1", "/Users/minhquando/Desktop/anacostia/anacostia_pipeline/resource/folder1")
    DAG().start()