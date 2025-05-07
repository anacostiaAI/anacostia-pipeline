import time

web_tests_path = "./testing_artifacts/frontend"
path = f"{web_tests_path}/webserver_test"
metadata_store_path = f"{path}/metadata_store"
haiku_data_store_path = f"{path}/haiku"


def create_file(file_path, content):
    try:
        with open(file_path, 'w') as file:
            file.write(content)
        print(f"File '{file_path}' created successfully.")
    except Exception as e:
        print(f"Error creating the file: {e}")


time.sleep(6)
for i in range(10):
    create_file(f"{haiku_data_store_path}/test_file{i}.txt", f"test file {i}")
    time.sleep(1.5)