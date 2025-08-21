import time
from utils import *



if __name__ == "__main__":
    root_path = f"./root-artifacts"
    root_input_path = f"{root_path}/input_artifacts"
    haiku_data_store_path = f"{root_input_path}/haiku"

    leaf_path = f"./leaf-artifacts"
    leaf_input_path = f"{leaf_path}/input_artifacts"
    shakespeare_data_store_path = f"{leaf_input_path}/shakespeare"

    for i in range(10, 20):
        create_file(f"{shakespeare_data_store_path}/test_leaf_file{i}.txt", f"test leaf file {i}")
        time.sleep(1.5)