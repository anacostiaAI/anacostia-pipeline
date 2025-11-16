import os
import string



PRINTABLE = (string.ascii_letters + string.digits + string.punctuation + " ").encode()

prep_artifacts = "./prep_artifacts"
input_store_path = f"{prep_artifacts}/input_store"


def write_random_data_mb(filepath: str, megabytes: int, chunk_size: int = 1024 * 1024):
    """
    Write `megabytes` of random printable bytes to `filepath`.
    """
    total_bytes = megabytes * 1024 * 1024
    n = len(PRINTABLE)

    with open(filepath, "wb") as f:
        written = 0
        while written < total_bytes:
            remaining = total_bytes - written
            size = min(chunk_size, remaining)

            # generate random indexes into PRINTABLE
            idx = os.urandom(size)
            chunk = bytes(PRINTABLE[b % n] for b in idx)

            f.write(chunk)
            written += size


def get_file_size_mb(filepath: str) -> float:
    """
    Return the file size in megabytes (MB).
    """
    size_bytes = os.path.getsize(filepath)
    size_mb = size_bytes / (1024 * 1024)
    return size_mb


NUM_FILES = 10
#MEGABYTES_PER_FILE = 10    # 10 MB
MEGABYTES_PER_FILE = 100   # 100 MB
#MEGABYTES_PER_FILE = 250
#MEGABYTES_PER_FILE = 500   # 500 MB
#MEGABYTES_PER_FILE = 1024  # 1 GB


if not os.path.exists(input_store_path):
    os.makedirs(input_store_path)

    for i in range(NUM_FILES):
        filepath = f"{input_store_path}/test_file{i}.txt"
        print(f"Creating {MEGABYTES_PER_FILE}MB file {filepath}...")
        write_random_data_mb(filepath, MEGABYTES_PER_FILE)
else:
    print("prep data files have already beeen generated")

    for filepath in os.listdir(input_store_path):
        filepath = f"{input_store_path}/{filepath}"
        if get_file_size_mb(filepath) != MEGABYTES_PER_FILE:
            print("File size mismatch. Regenerating files...")
            for i in range(NUM_FILES):
                filepath = f"{input_store_path}/test_file{i}.txt"
                write_random_data_mb(filepath, MEGABYTES_PER_FILE)