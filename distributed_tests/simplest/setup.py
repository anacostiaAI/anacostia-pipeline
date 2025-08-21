import shutil
import os
import logging



testing_artifacts = "./testing_artifacts"



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
setup_path(testing_artifacts)
print("setup complete")
