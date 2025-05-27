from typing import List, Union, Any, Optional, Callable
from logging import Logger
import os
import hashlib

from fastapi import Request, HTTPException, Header
from fastapi.responses import FileResponse, JSONResponse
import httpx

from anacostia_pipeline.nodes.resources.api import BaseResourceServer, BaseResourceClient



class FilesystemStoreServer(BaseResourceServer):
    def __init__(self, node, client_url: str, host = "127.0.0.1", port = 8000, loggers: Union[Logger, List[Logger]]  = None, *args, **kwargs):
        super().__init__(node, client_url, host, port, loggers, *args, **kwargs)
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

                # Compute SHA-256 hash of the file
                file_hash = self.node.hash_file(artifact_path)
                headers = {"X-File-Hash": file_hash}

                # Return the file as a response
                self.log(f"Sending file: {artifact_path}", level="INFO")
                return FileResponse(path=artifact_path, media_type="application/octet-stream", headers=headers)

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

                # Compute SHA-256 hash of the file
                expected_hash = request.headers.get("x-file-hash")
                if not expected_hash:
                    raise HTTPException(status_code=500, detail="Missing file hash in response headers")
                
                # Verify file hash
                actual_hash = self.node.hash_file(file_path)
                if actual_hash != expected_hash:
                    self.log(f"Hash mismatch! Expected: {expected_hash}, Actual: {actual_hash}", level="ERROR")
                    raise HTTPException(status_code=500, detail="Downloaded file hash mismatch")

                # enter the uploaded file into the metadata store
                await self.node.record_current(x_filename, hash=actual_hash, hash_algorithm="sha256")
                
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



class FilesystemStoreClient(BaseResourceClient):
    def __init__(self, storage_directory: str, client_name: str, client_host = "127.0.0.1", client_port = 8000, server_url = None, loggers = None, *args, **kwargs):
        super().__init__(client_name=client_name, client_host=client_host, client_port=client_port, server_url=server_url, loggers=loggers, *args, **kwargs)

        self.storage_directory = f"{storage_directory}/{client_name}"
    
        if os.path.exists(self.storage_directory) is False:
            os.makedirs(self.storage_directory)
        
    def hash_file(self, filepath: str, chunk_size: int = 8192) -> str:
        sha256 = hashlib.sha256()
        with open(filepath, 'rb') as f:
            while chunk := f.read(chunk_size):
                sha256.update(chunk)
        return sha256.hexdigest()
    
    async def get_artifact(self, filepath: str) -> Any:
        local_filepath = os.path.join(self.storage_directory, filepath)

        try:
            async with httpx.AsyncClient() as client:

                # Stream the response to handle large files efficiently
                url = f"{self.get_server_url()}/get_artifact/{filepath}"
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
                        
                        # Get expected hash from header
                        expected_hash = response.headers.get("x-file-hash")
                        if not expected_hash:
                            raise HTTPException(status_code=500, detail="Missing file hash in response headers")
                        
                        # Verify file hash
                        actual_hash = self.hash_file(local_filepath)
                        if actual_hash != expected_hash:
                            self.log(f"Hash mismatch! Expected: {expected_hash}, Actual: {actual_hash}", level="ERROR")
                            raise HTTPException(status_code=500, detail="Downloaded file hash mismatch")

                        self.log(f"File downloaded successfully: {local_filepath}", level="INFO")
                        return True

        except Exception as e:
            self.log(f"Error: An exception occurred while downloading the file: {str(e)}", level="ERROR")
            raise HTTPException(status_code=500, detail=f"Error: An exception occurred while downloading the file: {str(e)}")

    async def upload_file(self, filepath: str, remote_path: str = None):
        """
        Upload a file back to the FilesystemStoreRPCserver on the root pipeline.
        Args:
            filepath (str): Path to the file to be uploaded. Note that this path is relative to the storage directory of the client.
            remote_path (str): Path where the file will be stored on the root pipeline. Note that this path is relative to the storage directory of the server.
        Raises:
            FileNotFoundError: If the file does not exist at the specified path relative to the storage directory of the client.
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

            file_hash = self.hash_file(filepath)
            
            # Set up headers with file metadata
            headers = {
                "X-Filename": filename,
                "X-File-Hash": file_hash,
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
                    f"{self.get_server_url()}/upload_stream",
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
    
    # TODO: add the load_artifact method to BaseResourceRPCclient
    def load_artifact(self, filepath: str, load_fn: Callable[[str, Any], Any], *args, **kwargs) -> Any:
        """
        Load an artifact from the specified path relative to the resource_path.

        Args:
            filepath (str): Path of the artifact to load, relative to the resource_path.
                            Example: "data/file.txt" will load the file at resource_path/data/file.txt.
            load_fn (Callable[[str, Any], Any]): A function that takes the full path to the artifact and additional arguments,
                                                 and returns the loaded artifact. 
                                                 Note: if you subclass FilesystemStoreClient, make sure to include a default argument for load_fn.
            *args: Additional positional arguments to pass to `load_fn`.
            **kwargs: Additional keyword arguments to pass to `load_fn`.

        Returns:
            Any: The loaded artifact.

        Raises:
            FileNotFoundError: If the artifact file does not exist.
            Exception: If an error occurs during loading.
        """
        
        artifact_save_path = os.path.join(self.storage_directory, filepath)
        if os.path.exists(artifact_save_path) is False:
            raise FileExistsError(f"File '{artifact_save_path}' does not exists.")

        try:
            return load_fn(artifact_save_path, *args, **kwargs)
        except Exception as e:
            self.log(f"Failed to load artifact '{filepath}': {e}", level="ERROR")
            raise e