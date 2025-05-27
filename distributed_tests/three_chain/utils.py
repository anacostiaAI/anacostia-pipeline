import os


root_input_artifacts = "./root-artifacts/input_artifacts"
root_output_artifacts = "./root-artifacts/output_artifacts"
leaf1_input_artifacts = "./leaf-1-artifacts/input_artifacts"
leaf1_output_artifacts = "./leaf-1-artifacts/output_artifacts"
leaf2_input_artifacts = "./leaf-2-artifacts/input_artifacts"
leaf2_output_artifacts = "./leaf-2-artifacts/output_artifacts"
testing_artifacts = "./testing_artifacts"


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