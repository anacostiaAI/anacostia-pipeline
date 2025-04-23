import time
from utils import *



if __name__ == "__main__":
    path = f"./root-artifacts"
    input_path = f"{path}/input_artifacts"
    haiku_data_store_path = f"{input_path}/haiku"

    time.sleep(6)
    for i in range(10):
        create_file(f"{haiku_data_store_path}/test_file{i}.txt", f"test file {i}")
        time.sleep(1.5)