import time



logging_tests_path = "./testing_artifacts/logging_tests"
data_store_path = f"{logging_tests_path}/data_store"


def create_file(file_path, content):
    try:
        with open(file_path, 'w') as file:
            file.write(content)
        print(f"File '{file_path}' created successfully.")
    except Exception as e:
        print(f"Error creating the file: {e}")


for i in range(10):
    create_file(f"{data_store_path}/test_file{i}.txt", f"test file {i}")
    time.sleep(1.5)