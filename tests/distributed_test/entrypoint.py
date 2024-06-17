import shutil
import subprocess
import os
import time
import sys



def setup_path(path: str):
    if os.path.exists(path) is True:
        shutil.rmtree(path)
    os.makedirs(path)


root_input_artifacts = "./root-service/input_artifacts"
root_output_artifacts = "./root-service/output_artifacts"
leaf_input_artifacts = "./leaf-service/input_artifacts"
leaf_output_artifacts = "./leaf-service/output_artifacts"
testing_artifacts = "./testing_artifacts"

# clean up artifacts from old tests and recreate folders to set up testing environment for new test
setup_path(root_input_artifacts)
setup_path(root_output_artifacts)
setup_path(leaf_input_artifacts)
setup_path(leaf_output_artifacts)
setup_path(testing_artifacts)

# spin up docker containers in the background
subprocess.Popen("docker compose up --detach", shell=True)

# wait for graph to be set up
...

# begin tests (create a daemon process or thread to do run the test)
...

# on Ctrl+C, stop containers
while True:
    try:
        time.sleep(0.5)
    except KeyboardInterrupt:
        process = subprocess.Popen("docker compose down", shell=True)
        process.wait()
        sys.exit(0)