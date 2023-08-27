import time
import os
import sys
from threading import Thread
import numpy as np

sys.path.append('..')
sys.path.append('../anacostia_pipeline')
from anacostia_pipeline.engine.node import BaseNode
from anacostia_pipeline.engine.constants import Status


def get_log_messages(log_path: str, log_level: str = "INFO"):
    log_messages = []
    
    try:
        # Read the log file and extract log messages
        with open(log_path, 'r') as file:
            for line in file:
                parts = line.strip().split(' - ', 2)
                if parts[1] == log_level:
                    log_messages.append(parts[2])
                    
    except FileNotFoundError:
        print(f"Log file '{log_path}' not found.")

    except Exception as e:
        print(f"Error reading log file: {e}")
    
    return log_messages


def create_file(file_path, content):
    try:
        with open(file_path, 'w') as file:
            file.write(content)
        print(f"File '{file_path}' created successfully.")
    except Exception as e:
        print(f"Error creating the file: {e}")


def delete_file(file_path):
    try:
        os.remove(file_path)
        print(f"File '{file_path}' deleted successfully.")
    except Exception as e:
        print(f"Error deleting the file: {e}")


def get_time(log_path: str, log_level: str = "INFO"):
    timestamps = []

    try:
        # Read the log file and extract log messages
        with open(log_path, 'r') as file:
            for line in file:
                parts = line.strip().split(' - ', 2)
                if parts[1] == log_level:
                    timestamps.append(parts[0])

    except FileNotFoundError:
        print(f"Log file '{log_path}' not found.")

    except Exception as e:
        print(f"Error reading log file: {e}")  
    
    return timestamps


def get_time_delta(start_time: str, end_time: str):
    start_time = time.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    end_time = time.strptime(end_time, "%Y-%m-%d %H:%M:%S")
    return time.mktime(end_time) - time.mktime(start_time)


def run_node(node: BaseNode):
    node.set_status(Status.RUNNING)
    thread = Thread(target=node.run)
    thread.start()
    return thread


def stop_node(node: BaseNode, thread: Thread):
    node.set_status(Status.STOPPING)
    thread.join()
    node.teardown()


def create_numpy_file(file_path: str, shape: tuple = (10, 3)):
    array = np.zeros(shape)

    for i in range(array.shape[0]):
        array[i, :] = i

    np.save(file_path, array)


def create_array(shape: tuple = (10, 3)):
    array = np.zeros(shape)

    for i in range(array.shape[0]):
        array[i, :] = i

    return array


if __name__ == '__main__':
    array = np.zeros((10, 3))

    for i in range(array.shape[0]):
        array[i, :] = i

    print(array)