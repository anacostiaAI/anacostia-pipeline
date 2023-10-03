import time
import os
import sys
from threading import Thread
import numpy as np
import random
from typing import Tuple

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


def extract_npz(input_path: str, output_dir: str):
    if os.path.exists(output_dir) is False:
        os.makedirs(output_dir)

    # Load the .npz file
    data = np.load(input_path)

    # Get the names of the arrays
    array_names = data.files

    # Loop through the arrays
    for name in array_names:

        # Get the array
        array = data[name]

        # Construct output .npy filename
        output_file = name + '.npy'

        # Save the array as .npy file
        np.save(os.path.join(output_dir, output_file), array)


def split_numpy_file(
    filepath: str, 
    output_dir: str, 
    index_splits: Tuple[int] = None,
    time_delay: int = None
):
    if os.path.exists(output_dir) is False:
        os.makedirs(output_dir)

    array = np.load(filepath)
    num_samples = array.shape[0]
    splits = [split for split in index_splits]
    splits = splits + [num_samples]

    start = 0
    for index in splits:
        end = index 
        
        # create subarray
        chunk = array[start:end]

        # create path for saving subarray
        num_files = len(os.listdir(output_dir))
        filename = filepath.split("/")[-1]
        filename = filename.split(".")
        filename = f"{filename[0]}_{num_files}.npy"
        path = os.path.join(output_dir, filename)

        # saving the subarray
        np.save(path, chunk)
        print(f"saved chunk {(start, end)} in path {path}")

        # reset for next iteration
        start = end + 1
        if time_delay is not None:
            time.sleep(time_delay)
        

def combine_files(files_dict: dict, output_dir: str):
    for key, value in files_dict.items():
        images = np.load(key)
        labels = np.load(value)

        filename = key.split("/")[-1]
        filename, extension = filename.split(".")
        filename = filename.replace("_images_", "_")
        save_path = os.path.join(output_dir, f"{filename}.npz")

        print(f"Saving {save_path}")
        np.savez(save_path, val_images=images, val_labels=labels)


import pandas as pd
import threading
import matplotlib
matplotlib.use('Agg') 
import matplotlib.pyplot as plt

#def create_table(data_dict: dict, output_path: str):
def create_table(**kwargs):
    df = pd.DataFrame.from_dict(data_dict, orient='index').reset_index()
    df.columns = ['Attribute', 'Value']

    def save_figure(df):
        plt.table(cellText=df.values, colLabels=df.columns, loc='center')
        plt.axis('off')
        plt.savefig(output_path, bbox_inches='tight', pad_inches=0)

    #thread = threading.Thread(target=save_figure, args=(df,))
    #thread.start()
    #thread.join()


if __name__ == '__main__':
    #extract_npz("./testing_artifacts/retinamnist.npz", "./testing_artifacts/data_store")
    """
    split_numpy_file(
        filepath="./testing_artifacts/data_store/test_images.npy", 
        output_dir="./testing_artifacts/data_store/test_splits", 
        index_splits=(100, 200, 300),
        time_delay=0.5
    )
    combine_files(
        files_dict={
            "./testing_artifacts/data_store/val_splits/val_images_4.npy": "./testing_artifacts/data_store/val_splits/val_labels_4.npy",
            "./testing_artifacts/data_store/val_splits/val_images_5.npy": "./testing_artifacts/data_store/val_splits/val_labels_5.npy",
            "./testing_artifacts/data_store/val_splits/val_images_6.npy": "./testing_artifacts/data_store/val_splits/val_labels_6.npy",
            "./testing_artifacts/data_store/val_splits/val_images_7.npy": "./testing_artifacts/data_store/val_splits/val_labels_7.npy"
        },
        output_dir="./testing_artifacts/data_store/val_splits"
    )
    """

    

    data_dict={"Accuracy": 0.96, "Precision": 0.96, "Recall": 0.96, "F1": 0.96},
    output_path="./testing_artifacts/test.png"
    
    thread = threading.Thread(target=create_table, args=(data_dict, output_path))
    thread.start()
    thread.join()