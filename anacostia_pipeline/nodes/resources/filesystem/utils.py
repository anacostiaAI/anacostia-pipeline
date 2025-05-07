import os
from contextlib import contextmanager

if os.name == 'nt':  # Windows
    import msvcrt
else:  # Unix-like systems (Linux, macOS)
    import fcntl



@contextmanager
def locked_file(filename, mode='r'):
    with open(filename, mode) as file:
        try:
            if os.name == 'nt':  # Windows
                if mode.startswith('r'):  # Shared lock for reading
                    msvcrt.locking(file.fileno(), msvcrt.LK_NBRLCK, os.path.getsize(filename))
                else:  # Exclusive lock for writing
                    msvcrt.locking(file.fileno(), msvcrt.LK_LOCK, os.path.getsize(filename))
            
            else:  # Unix-like systems
                if mode.startswith('r'):  # Shared lock for reading
                    fcntl.flock(file.fileno(), fcntl.LOCK_SH)
                else:  # Exclusive lock for writing
                    fcntl.flock(file.fileno(), fcntl.LOCK_EX)
            
            yield file
        
        finally:
            # Unlock the file
            if os.name == 'nt':  # Windows
                msvcrt.locking(file.fileno(), msvcrt.LK_UNLCK, os.path.getsize(filename))
            else:  # Unix-like systems
                fcntl.flock(file.fileno(), fcntl.LOCK_UN)

        # use a shared lock (fcntl.LOCK_SH) for reading:
        # - allows multiple processes to acquire a shared lock for reading
        # - multiple readers can access the file simultaneously
        # - prevents any process from acquiring an exclusive lock (fcntl.LOCK_EX) for writing while readers have the file open

        # use an exclusive lock (fcntl.LOCK_EX) for writing
        # - allows only one process to acquire an exclusive lock for writing
        # - prevents any other process from acquiring a shared or exclusive lock for reading or writing
        # - ensures that only one writer can modify the file at a time, and no readers can access it during the write operation