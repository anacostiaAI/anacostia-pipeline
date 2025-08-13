from typing import List, Union
from logging import Logger
import asyncio

from fastapi import HTTPException
from fastapi.responses import JSONResponse

from anacostia_pipeline.nodes.api import BaseClient, BaseServer



class BaseResourceServer(BaseServer):
    def __init__(
        self, 
        node, 
        client_url, 
        host = "127.0.0.1", 
        port = 8000, 
        loggers: Union[Logger, List[Logger]]  = None, 
        ssl_keyfile: str = None, 
        ssl_certfile: str = None, 
        ssl_ca_certs: str = None, 
        *args, **kwargs
    ):
        super().__init__(
            node=node,
            client_url=client_url,
            host=host,
            port=port,
            loggers=loggers,
            ssl_keyfile=ssl_keyfile,
            ssl_certfile=ssl_certfile, 
            ssl_ca_certs=ssl_ca_certs, 
            *args, **kwargs
        )

        @self.get("/get_num_artifacts/")
        async def get_num_artifacts(state: str):
            num_artifacts = self.node.get_num_artifacts(state)
            try:
                return JSONResponse(content={"num_artifacts": num_artifacts}, status_code=200)
            except Exception as e:
                return JSONResponse(content={"error": f"An error occurred while getting the number of artifacts: {str(e)}"}, status_code=500)
        
        @self.get("/list_artifacts/")
        async def list_artifacts(state: str):
            try:
                artifacts = self.node.list_artifacts(state)
                return JSONResponse(content={"artifacts": artifacts}, status_code=200)
            except Exception as e:
                return JSONResponse(content={"error": f"An error occurred while listing artifacts: {str(e)}"}, status_code=500)



class BaseResourceClient(BaseClient):
    def __init__(
        self, 
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

    def get_num_artifacts(self, state: str = "all") -> int:
        """
        Get the number of artifacts in the storage directory.
        Returns:
            int: The number of artifacts in the storage directory.
        """
        
        async def _get_num_artifacts(state: str):
            try:
                response = await self.client.get(f"/get_num_artifacts/?state={state}")
                if response.status_code == 200:
                    return response.json()["num_artifacts"]
                else:
                    self.log(f"Error: Received status code {response.status_code}", level="ERROR")
                    raise HTTPException(status_code=response.status_code, detail=f"Error: {response.text}")

            except Exception as e:
                self.log(f"Error: An exception occurred while getting the number of artifacts: {str(e)}", level="ERROR")
                raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

        task = asyncio.run_coroutine_threadsafe(_get_num_artifacts(state), self.loop)
        return task.result()

    def list_artifacts(self, state: str = "all") -> List[str]:
        """
        List all artifacts in the storage directory.
        Returns:
            List[str]: A list of artifact names.
        """

        async def _list_artifacts(state: str):
            try:
                response = await self.client.get(f"/list_artifacts/?state={state}")
                if response.status_code == 200:
                    return response.json()["artifacts"]
                else:
                    self.log(f"Error: Received status code {response.status_code}", level="ERROR")
                    raise HTTPException(status_code=response.status_code, detail=f"Error: {response.text}")
            except Exception as e:
                self.log(f"Error: An exception occurred while listing artifacts: {str(e)}", level="ERROR")
                raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
        
        task = asyncio.run_coroutine_threadsafe(_list_artifacts(state), self.loop)
        return task.result()