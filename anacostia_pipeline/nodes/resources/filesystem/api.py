from typing import List, Union, Any, Optional, Iterator
from contextlib import contextmanager
from logging import Logger
import os
import hashlib
import asyncio

from fastapi import Request, HTTPException, Header
from fastapi.responses import FileResponse, JSONResponse

from anacostia_pipeline.nodes.resources.api import BaseResourceServer, BaseResourceClient



class FilesystemStoreServer(BaseResourceServer):
    def __init__(
        self, 
        node, 
        client_url: str, 
        host = "127.0.0.1", 
        port = 8000, 
        loggers: Union[Logger, List[Logger]]  = None, 
        ssl_keyfile: str = None, 
        ssl_certfile: str = None, 
        ssl_ca_certs: str = None, 
        *args, **kwargs
    ):
        super().__init__(
            node, client_url, host, port, loggers, ssl_keyfile=ssl_keyfile, ssl_certfile=ssl_certfile, ssl_ca_certs=ssl_ca_certs, *args, **kwargs
        )
        self.resource_path: str = node.resource_path

        @self.post("/mark_using/{filepath:path}")
        async def mark_using(filepath: str):
            self.log(f"Received request to mark using: {filepath}", level="INFO")
            try:
                self.node.mark_using(filepath)
                return JSONResponse(
                    content={"status": f"Artifact '{filepath}' marked as using."},
                    status_code=200
                )
            except Exception as e:
                self.log(f"Error marking using: {str(e)}", level="ERROR")
                raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

        @self.post("/mark_used/{filepath:path}")
        async def mark_used(filepath: str):
            self.log(f"Received request to mark used: {filepath}", level="INFO")
            try:
                self.node.mark_used(filepath)
                return JSONResponse(
                    content={"status": f"Artifact '{filepath}' marked as used."},
                    status_code=200
                )
            except Exception as e:
                self.log(f"Error marking used: {str(e)}", level="ERROR")
                raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

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
                self.node.record_produced_artifact(x_filename, hash=actual_hash, hash_algorithm="sha256")
                
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
    def __init__(
        self, 
        storage_directory: str, 
        client_name: str, 
        client_host = "127.0.0.1", 
        client_port = 8000, 
        server_url = None, 
        loggers = None, 
        ssl_keyfile: str = None, 
        ssl_certfile: str = None, 
        ssl_ca_certs: str = None, 
        *args, **kwargs
    ):
        super().__init__(
            client_name=client_name, 
            client_host=client_host, 
            client_port=client_port, 
            server_url=server_url, 
            loggers=loggers, 
            ssl_keyfile=ssl_keyfile, 
            ssl_certfile=ssl_certfile, 
            ssl_ca_certs=ssl_ca_certs, 
            *args, **kwargs
        )

        self.storage_directory = f"{storage_directory}/{client_name}"
    
        if os.path.exists(self.storage_directory) is False:
            os.makedirs(self.storage_directory)
        
    def hash_file(self, filepath: str, chunk_size: int = 8192) -> str:
        sha256 = hashlib.sha256()
        with open(filepath, 'rb') as f:
            while chunk := f.read(chunk_size):
                sha256.update(chunk)
        return sha256.hexdigest()
    
    def download_artifact(self, filepath: str) -> Any:
        """
        Download an artifact from the FilesystemStoreRPCserver on the root pipeline.
        Args:
            filepath (str): Path of the artifact to download, relative to the resource_path.
                            Example: "data/file.txt" will download the file from resource_path/data/file.txt.
        Raises:
            HTTPException: If the response code from /get_artifact is not 200.
        """

        async def _get_artifact(local_filepath: str, filepath: str):

            # Stream the response to handle large files efficiently
            url = f"/get_artifact/{filepath}"
            async with self.client.stream("GET", url) as response:
                if response.status_code != 200:
                    self.log(f"Error in download_artifact: Server returned status code {response.status_code}", level="ERROR")
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
            
        try:
            local_filepath = os.path.join(self.storage_directory, filepath)
            asyncio.run_coroutine_threadsafe(_get_artifact(local_filepath, filepath), self.loop)

        except Exception as e:
            self.log(f"Error: An exception occurred while downloading the file: {str(e)}", level="ERROR")
            raise HTTPException(status_code=500, detail=f"Error: An exception occurred while downloading the file: {str(e)}")

    def upload_artifact(self, filepath: str, remote_path: str = None):
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
            async def _upload_file():
                response = await self.client.post(
                    f"/upload_stream",
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
                    self.log(f"Error in upload_artifact: Received status code {response.status_code}", level="ERROR")
                    self.log(f"Response: {response.text}", level="ERROR")
                    raise HTTPException(status_code=response.status_code, detail=f"Error: {response.text}")
                
            # Run the upload in an async context
            asyncio.run_coroutine_threadsafe(_upload_file(), self.loop)
                
        except Exception as e:
            self.log(f"Error: An exception occurred while sending the file: {str(e)}", level="ERROR")
            raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    
    def mark_using(self, filepath: str) -> None:
        """
        Mark an artifact as used on the FilesystemStoreRPCserver on the root pipeline.
        Args:
            filepath (str): Path of the artifact to mark as used, relative to the resource_path.
                            Example: "data/file.txt" will mark the file at resource_path/data/file.txt as used.
        Raises:
            HTTPException: If the response code from /mark_using is not 200.
        """

        async def _mark_using(filepath: str):
            url = f"/mark_using/{filepath}"
            response = await self.client.post(url)
            if response.status_code != 200:
                self.log(f"Error in mark_using: Server returned status code {response.status_code}", level="ERROR")
                self.log(f"Response: {await response.text()}", level="ERROR")
                raise HTTPException(status_code=response.status_code, detail=f"Error: Server returned status code {await response.text()}")
            else:
                self.log(f"Artifact marked as used successfully: {filepath}", level="INFO")
                return True

        try:
            asyncio.run_coroutine_threadsafe(_mark_using(filepath), self.loop)

        except Exception as e:
            self.log(f"Error: An exception occurred while marking the artifact as used: {str(e)}", level="ERROR")
            raise HTTPException(status_code=500, detail=f"Error: An exception occurred while marking the artifact as used: {str(e)}")
    
    def mark_used(self, filepath: str) -> None:
        """
        Mark an artifact as used on the FilesystemStoreRPCserver on the root pipeline.
        Args:
            filepath (str): Path of the artifact to mark as used, relative to the resource_path.
                            Example: "data/file.txt" will mark the file at resource_path/data/file.txt as used.
        Raises:
            HTTPException: If the response code from /mark_used is not 200.
        """

        async def _mark_used(filepath: str):
            url = f"/mark_used/{filepath}"
            response = await self.client.post(url)
            if response.status_code != 200:
                self.log(f"Error in mark_used: Server returned status code {response.status_code}", level="ERROR")
                self.log(f"Response: {await response.text()}", level="ERROR")
                raise HTTPException(status_code=response.status_code, detail=f"Error: Server returned status code {await response.text()}")
            else:
                self.log(f"Artifact marked as used successfully: {filepath}", level="INFO")
                return True

        try:
            asyncio.run_coroutine_threadsafe(_mark_used(filepath), self.loop)

        except Exception as e:
            self.log(f"Error: An exception occurred while marking the artifact as used: {str(e)}", level="ERROR")
            raise HTTPException(status_code=500, detail=f"Error: An exception occurred while marking the artifact as used: {str(e)}")
    
    @contextmanager
    def load_artifact(self, filepath: str) -> Iterator[Any]:
        """
        Context manager to load an artifact from the specified path relative to the resource_path.

        Args:
            filepath (str): Path of the artifact to load, relative to the resource_path.
                            Example: "data/file.txt" will load the file at resource_path/data/file.txt.
                            **IMPORTANT NOTE**: make sure filepath does not start with a leading '/'.

        Returns:
            Any: The loaded artifact.

        Raises:
            FileNotFoundError: If the artifact file does not exist.
            Exception: If an error occurs during loading.
        
        ## Usage patterns:
        1. Loading a file
        ```
        fs_store = FilesystemStoreNode(...)

        with fs_store.load_artifact("data/file.txt") as full_path:
            with open(full_path, "r", encoding="utf-8") as f:
                buf = f.read()
        ```
        2. Loading a PyTorch model
        ```
        import torch
        
        fs_store = FilesystemStoreNode(...)

        with fs_store.load_artifact("models/model.pt") as full_path:
            # load the model weights
            torch.load(full_path, map_location="cpu")
            
            # use the model here
            model.eval()
            ...
        ```
        """

        # Note: if self.storage_directory = "/path/to/dir" and filepath = "subdir/file.txt", then
        # os.path.join(self.storage_directory, filepath) will give "/path/to/dir/subdir/file.txt"
        # if self.storage_directory = "/path/to/dir/" and filepath = "/path/to/dir/subdir/file.txt", then
        # os.path.join(self.storage_directory, filepath) will still give "/path/to/dir/subdir/file.txt"
        artifact_path = os.path.join(self.storage_directory, filepath)
        if not os.path.exists(artifact_path):
            raise FileNotFoundError(f"File '{artifact_path}' does not exist.")

        try:
            relative_path = os.path.relpath(artifact_path, self.storage_directory)

            self.mark_using(relative_path)

            # yield the full path to the artifact for the caller to use
            yield artifact_path

            self.mark_used(relative_path)

        except Exception as e:
            self.log(f"Failed to load artifact '{filepath}': {e}", level="ERROR")
            raise
