import shutil
import os
import logging

from utils import *


def setup_path(path: str):
    try:
        # Attempt to delete the directory and all its contents
        shutil.rmtree(path)
        print(f"Successfully deleted '{path}' directory and its contents.")
    
    except FileNotFoundError:
        print(f"Directory '{path}' not found.")
    
    except Exception as e:
        print(f"Error deleting directory '{path}': {e}")
    
    finally:
        print(f"recreating {path}")
        os.makedirs(path)



# clean up artifacts from old tests and recreate folders to set up testing environment for new test
print("setup started")

setup_path(root_input_artifacts)
setup_path(root_output_artifacts)
setup_path(leaf1_input_artifacts)
setup_path(leaf1_output_artifacts)
setup_path(leaf2_input_artifacts)
setup_path(leaf2_output_artifacts)
setup_path(testing_artifacts)

print("setup complete")


log_path = f"{testing_artifacts}/anacostia.log"
logging.basicConfig(
    level=logging.DEBUG,
    format='ROOT %(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='a'
)
logger = logging.getLogger(__name__)
