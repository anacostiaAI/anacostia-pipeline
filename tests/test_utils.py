import time
import os


def get_log_messages(log_path: str, log_level: str = "INFO"):
    log_messages = []
    
    try:
        # Read the log file and extract log messages
        with open(log_path, 'r') as file:
            for line in file:
                parts = line.strip().split(' - ', 2)
                if parts[1] == log_level:
                    log_messages.append(parts[2])
                    
    except FileNotFoundError:
        print(f"Log file '{log_path}' not found.")

    except Exception as e:
        print(f"Error reading log file: {e}")
    
    return log_messages


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


def get_time(log_path: str, log_level: str = "INFO"):
    timestamps = []

    try:
        # Read the log file and extract log messages
        with open(log_path, 'r') as file:
            for line in file:
                parts = line.strip().split(' - ', 2)
                if parts[1] == log_level:
                    timestamps.append(parts[0])

    except FileNotFoundError:
        print(f"Log file '{log_path}' not found.")

    except Exception as e:
        print(f"Error reading log file: {e}")  
    
    return timestamps


def get_time_delta(start_time: str, end_time: str):
    start_time = time.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    end_time = time.strptime(end_time, "%Y-%m-%d %H:%M:%S")
    return time.mktime(end_time) - time.mktime(start_time)


if __name__ == '__main__':
    times = get_time(log_path="./testing_artifacts/app.log", log_level="INFO")
    time_delta = get_time_delta(times[2], times[3])
    print(times)
    print(time_delta)