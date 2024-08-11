import time
import os
import sys
from threading import Thread
import numpy as np
import random
from typing import Tuple

sys.path.append('..')
sys.path.append('../anacostia_pipeline')
from anacostia_pipeline.engine.base import BaseActionNode, BaseResourceNode, BaseNode


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


def run_computational_task(node: BaseNode, duration_seconds: int):
    node.log(f"Node {node.name} is starting a computationally intensive task.")

    # Record the start time
    start_time = time.time()
    result = 0
    while time.time() - start_time < duration_seconds:
        for i in range(10**7):
            result += i

    node.log(f"Node {node.name} completed the computationally intensive task.")