import shutil
import os



def setup_path(path: str):
    if os.path.exists(path) is True:
        shutil.rmtree(path)
    os.makedirs(path)



def delete_files_in_folder(folder_path):
    try:
        # Iterate over all files in the folder
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            
            # Check if the path is a file (not a directory)
            if os.path.isfile(file_path):
                os.remove(file_path)  # Delete the file
                print(f"Deleted: {file_path}")
            else:
                print(f"Skipped: {file_path} (not a file)")

        print(f"All files deleted from {folder_path}")

    except Exception as e:
        print(f"Error deleting files: {e}")



root_input_artifacts = "/root-service/input_artifacts"
root_output_artifacts = "/root-service/output_artifacts"
leaf_input_artifacts = "/leaf-service/input_artifacts"
leaf_output_artifacts = "/leaf-service/output_artifacts"
testing_artifacts = "/testing_artifacts"

print("setup started")

# clean up artifacts from old tests and recreate folders to set up testing environment for new test
setup_path(root_input_artifacts)
setup_path(root_output_artifacts)
setup_path(leaf_input_artifacts)
setup_path(leaf_output_artifacts)
delete_files_in_folder(testing_artifacts)

print("setup complete")