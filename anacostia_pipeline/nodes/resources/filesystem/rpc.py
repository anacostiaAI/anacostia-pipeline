from typing import List, Union, Any
from logging import Logger
import os
from contextlib import contextmanager

if os.name == 'nt':  # Windows
    import msvcrt
else:  # Unix-like systems (Linux, macOS)
    import fcntl

from fastapi import Request
from fastapi.responses import FileResponse
import httpx

from anacostia_pipeline.nodes.rpc import BaseRPCCaller, BaseRPCCallee



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



class FilesystemStoreRPCCallee(BaseRPCCallee):
    def __init__(self, node, caller_url, host = "127.0.0.1", port = 8000, loggers: Union[Logger, List[Logger]]  = None, *args, **kwargs):
        super().__init__(node, caller_url, host, port, loggers, *args, **kwargs)

        @self.get("/get_artifact/{filepath:path}", response_class=FileResponse)
        async def get_artifact(filepath: str):
            self.log(f"Received request to get artifact: {filepath}", level="INFO")



class FilesystemStoreRPCCaller(BaseRPCCaller):
    def __init__(self, storage_directory: str, caller_name: str, caller_host = "127.0.0.1", caller_port = 8000, loggers = None, *args, **kwargs):
        super().__init__(caller_name, caller_host, caller_port, loggers, *args, **kwargs)

        self.storage_directory = f"{storage_directory}/{caller_name}"
    
        if os.path.exists(self.storage_directory) is False:
            os.makedirs(self.storage_directory)
        
    async def get_artifact(self, filepath: str) -> Any:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self.get_callee_url()}/get_artifact/{filepath}")
            if response.status_code == 200:
                artifact_path = os.path.join(self.storage_directory, os.path.basename(filepath))
                # TODO: write the response content to the local file
                self.log(f"Artifact saved to {artifact_path}", level="INFO")
            else:
                raise Exception(f"Failed to get artifact: {response.status_code} - {response.text}")

    # TODO: add the load_artifact method to BaseResourceRPCCaller
    def load_artifact(self, artifact_path: str) -> Any:
        with locked_file(artifact_path, "r") as file:
            return file.read()
