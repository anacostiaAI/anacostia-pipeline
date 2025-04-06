from typing import List, Union, Any, Optional
from logging import Logger
import os
from contextlib import contextmanager

if os.name == 'nt':  # Windows
    import msvcrt
else:  # Unix-like systems (Linux, macOS)
    import fcntl

from fastapi import Request, HTTPException, Header
from fastapi.responses import FileResponse, JSONResponse
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
        self.resource_path: str = node.resource_path

        @self.get("/get_artifact/{filepath:path}", response_class=FileResponse)
        async def get_artifact(filepath: str):
            self.log(f"Received request to get artifact: {filepath}", level="INFO")
            try:
                # validate the file path exists
                artifact_path = os.path.join(self.resource_path, filepath)
                if os.path.exists(artifact_path) is False:
                    self.log(f"Error: File not found - {artifact_path}", level="ERROR")
                    raise HTTPException(status_code=404, detail=f"Resource path not found: {artifact_path}")

                # Return the file as a response
                self.log(f"Sending file: {artifact_path}", level="INFO")
                return FileResponse(path=artifact_path, media_type="application/octet-stream")

            except HTTPException as e:
                self.log(f"HTTPException: {str(e)}", level="ERROR")
                raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
        
        @self.post("/upload_stream")
        async def upload_stream(request: Request, x_filename: Optional[str] = Header(None)):
            try:
                # Check if the file already exists
                file_path = os.path.join(self.resource_path, x_filename)
                if os.path.exists(file_path) is True:
                    self.log(f"Error: File already exists: {file_path}", level="ERROR")
                    raise HTTPException(status_code=409, detail=f"File already exists: {file_path}")
                
                # Create the directory if it doesn't exist
                folder_path = os.path.join(self.resource_path, os.path.dirname(x_filename))
                if os.path.exists(folder_path) is False:
                    os.makedirs(folder_path)
                
                # Stream the request body directly to a file
                content_length = request.headers.get("content-length")
                if content_length:
                    total_size = int(content_length)
                    bytes_received = 0
                else:
                    total_size = None
                    bytes_received = 0
                
                # Open the file and write chunks as they arrive
                with open(file_path, "wb") as f:
                    async for chunk in request.stream():
                        f.write(chunk)
                        bytes_received += len(chunk)

                        # Optional: Add progress logging here
                        if total_size:
                            progress = bytes_received / total_size * 100
                            self.log(f"Received: {bytes_received/1024/1024:.2f}MB / {total_size/1024/1024:.2f}MB ({progress:.1f}%)", level="INFO")

                # enter the uploaded file into the metadata store
                self.node.record_current(x_filename)
                
                return JSONResponse(
                    content={
                        "filename": x_filename,
                        "status": "File received and saved successfully",
                        "bytes_received": bytes_received,
                        "stored_path": str(file_path)
                    },
                    status_code=200
                )
            
            except Exception as e:
                return JSONResponse(
                    content={"error": f"An error occurred while receiving: {str(e)}"},
                    status_code=500
                )



class FilesystemStoreRPCCaller(BaseRPCCaller):
    def __init__(self, storage_directory: str, caller_name: str, caller_host = "127.0.0.1", caller_port = 8000, loggers = None, *args, **kwargs):
        super().__init__(caller_name, caller_host, caller_port, loggers, *args, **kwargs)

        self.storage_directory = f"{storage_directory}/{caller_name}"
    
        if os.path.exists(self.storage_directory) is False:
            os.makedirs(self.storage_directory)
        
    async def get_artifact(self, filepath: str) -> Any:
        local_filepath = os.path.join(self.storage_directory, filepath)

        try:
            async with httpx.AsyncClient() as client:

                # Stream the response to handle large files efficiently
                url = f"{self.get_callee_url()}/get_artifact/{filepath}"
                async with client.stream("GET", url) as response:
                    if response.status_code != 200:
                        self.log(f"Error: Server returned status code {response.status_code}", level="ERROR")
                        self.log(f"Response: {await response.text()}", level="ERROR")
                        raise HTTPException(status_code=response.status_code, detail=f"Error: Server returned status code {await response.text()}")
                    
                    else:
                        self.log(f"Downloading file from {url}...", level="INFO")

                        # Create the file and write the content chunk by chunk
                        with open(local_filepath, "wb") as f:
                            async for chunk in response.aiter_bytes():
                                f.write(chunk)
                        
                        self.log(f"File downloaded successfully: {local_filepath}", level="INFO")
                        return True

        except Exception as e:
            self.log(f"Error: An exception occurred while downloading the file: {str(e)}", level="ERROR")
            raise HTTPException(status_code=500, detail=f"Error: An exception occurred while downloading the file: {str(e)}")

    # TODO: add the load_artifact method to BaseResourceRPCCaller
    def load_artifact(self, artifact_path: str) -> Any:
        with locked_file(artifact_path, "r") as file:
            return file.read()

    async def upload_file(self, filepath: str, remote_path: str = None):
        """
        Upload a file back to the FilesystemStoreRPCCallee on the root pipeline.
        Args:
            filepath (str): Path to the file to be uploaded. Note that this path is relative to the storage directory of the caller.
            remote_path (str): Path where the file will be stored on the root pipeline. Note that this path is relative to the storage directory of the callee.
        Raises:
            FileNotFoundError: If the file does not exist at the specified path relative to the storage directory of the caller.
            HTTPException: If the response code from /upload_stream is not 200.
        """

        # Size of chunks to read and send (4MB)
        CHUNK_SIZE = 4 * 1024 * 1024

        filepath = os.path.join(self.storage_directory, filepath)

        # Check if file exists
        if os.path.exists(filepath) is False:
            self.log(f"Error: File not found - {filepath}", level="ERROR")
            raise FileNotFoundError(f"File not found: {filepath}")
        
        filename = remote_path.lstrip("/")          # remove leading slash
        
        try:
            filesize = os.path.getsize(filepath)

            self.log(f"Preparing to upload: {filename} ({filesize/1024/1024:.2f} MB)", level="INFO")
            
            # Set up headers with file metadata
            headers = {
                "X-Filename": filename,
                "Content-Type": "application/octet-stream",
                "Content-Length": str(filesize)
            }

            async def file_generator():
                """Generator function that yields chunks of the file"""
                with open(filepath, "rb") as f:
                    while chunk := f.read(CHUNK_SIZE):
                        yield chunk

                        # Optional: Add progress reporting
                        self.log(f"Sent chunk: {len(chunk)/1024/1024:.2f} MB", level="INFO")
            
            # Send the file using streaming upload
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.get_callee_url()}/upload_stream",
                    headers=headers,
                    content=file_generator(),
                    timeout=None  # Disable timeout for large uploads
                )
            
            # self.log the response
            if response.status_code == 200:
                self.log(f"Success: File {filename} sent successfully", level="INFO")
                response_data = response.json()
                self.log(f"remote storage path: {response_data['stored_path']}", level="INFO")
                return True
            else:
                self.log(f"Error: Received status code {response.status_code}", level="ERROR")
                self.log(f"Response: {response.text}", level="ERROR")
                raise HTTPException(status_code=response.status_code, detail=f"Error: {response.text}")
                
        except Exception as e:
            self.log(f"Error: An exception occurred while sending the file: {str(e)}", level="ERROR")
            raise HTTPException(status_code=500, detail=f"Error: {str(e)}")