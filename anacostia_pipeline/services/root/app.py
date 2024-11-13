import threading
import signal
import asyncio
import uuid
from logging import Logger
from contextlib import asynccontextmanager
from typing import List, Dict

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.routing import Mount

import uvicorn
import httpx
from pydantic import BaseModel

from anacostia_pipeline.nodes.node import NodeModel
from anacostia_pipeline.pipelines.root.pipeline import RootPipeline
from anacostia_pipeline.pipelines.root.app import RootPipelineApp
from anacostia_pipeline.nodes.network.receiver.app import ReceiverApp
from anacostia_pipeline.nodes.network.sender.node import SenderNode

from anacostia_pipeline.utils.constants import Work

from anacostia_pipeline.services.root.fragments import node_bar_closed, node_bar_open, node_bar_invisible, index_template


class RootServiceData(BaseModel):
    root_name: str
    leaf_host: str
    leaf_port: int
    root_host: str
    root_port: int
    sender_name: str
    receiver_name: str
    pipeline_id: str



class RootServiceApp(FastAPI):
    def __init__(self, name: str, pipeline: RootPipeline, host: str = "localhost", port: int = 8000, logger: Logger = None, *args, **kwargs):

        # lifespan context manager for spinning up and shutting down the service
        @asynccontextmanager
        async def lifespan(app: RootServiceApp):
            app.log(f"Opening client for service '{app.name}'")

            yield

            for route in app.routes:
                if isinstance(route, Mount):
                    if isinstance(route.app, RootPipelineApp):
                        app.log(f"Closing client for webserver '{route.app.name}'")
                        await route.app.client.aclose()

            app.log(f"Closing client for service '{app.name}'")
            # Note: we need to close the client after the lifespan context manager is done but for some reason await app.client.aclose() is throwing an error 
            # RuntimeError: unable to perform operation on <TCPTransport closed=True reading=False 0x121fa0fd0>; the handler is close
        
        super().__init__(lifespan=lifespan, *args, **kwargs)
        self.name = name
        self.host = host
        self.port = port
        self.pipeline = pipeline
        self.logger = logger
        self.connections = []
        self.leaf_ip_addresses = []
        self.client = httpx.AsyncClient()

        config = uvicorn.Config(self, host=self.host, port=self.port)
        self.server = uvicorn.Server(config)
        self.fastapi_thread = threading.Thread(target=self.server.run, name=name)
    
        # Connect to the leaf services
        asyncio.run(self.connect())

        # Note: we do not need to create a pipeline ID for the root service because there is only one root pipeline
        # leaf services create pipeline IDs because leaf services can connect to and spin up multiple pipelines for multiple services 
        pipeline_server = RootPipelineApp(name="pipeline", pipeline=self.pipeline, host=self.host, port=self.port)
        pipeline_server.client = httpx.AsyncClient()

        self.mount(f"/", pipeline_server)

    def log(self, message: str, level: str = "INFO"):
        if self.logger is not None:
            if level == "DEBUG":
                self.logger.debug(message)
            elif level == "INFO":
                self.logger.info(message)
            elif level == "WARNING":
                self.logger.warning(message)
            elif level == "ERROR":
                self.logger.error(message)
            elif level == "CRITICAL":
                self.logger.critical(message)
            else:
                raise ValueError(f"Invalid log level: {level}")
        else:
            print(f"{level}: {message}")
    
    async def connect(self):
        # Extract data about leaf pipelines from the sender nodes in the pipeline
        for node in self.pipeline.nodes:
            if isinstance(node, SenderNode):
                connection_dict = {
                    "root_name": self.name,
                    "leaf_host": node.leaf_host,
                    "leaf_port": node.leaf_port,
                    "root_host": self.host,
                    "root_port": self.port,
                    "sender_name": node.name,
                    "receiver_name": node.leaf_receiver,
                    "pipeline_id": ""
                }
                
                if any([connection_dict["receiver_name"] == connection["receiver_name"] for connection in self.connections]):
                    raise ValueError(f"Duplicate receiver name '{connection_dict['receiver_name']}' found in the pipeline")
                else:
                    self.connections.append(connection_dict)

            # Extract the leaf ip addresses from the connections
            for connection in self.connections:
                pipeline_ip_address = f"{connection['leaf_host']}:{connection['leaf_port']}"
                if pipeline_ip_address not in self.leaf_ip_addresses:
                    self.leaf_ip_addresses.append(f"{connection['leaf_host']}:{connection['leaf_port']}")
                    self.log(f"Root service '{self.name}' beginning connection protocol to leaf services at ip addresses: {self.leaf_ip_addresses}")

        try:
            # Note: don't use httpx.post here, it will throw an error "object Response can't be used in 'await' expression"
            # Instead, we use await self.client.post because we already have an httpx.AsyncClient() object 
            # created in the lifespan context manager in the AnacostiaService class.
            # See this video: https://www.youtube.com/watch?v=row-SdNdHFE

            # Send a /healthcheck request to each leaf service
            self.log("------------- Healthcheck started -------------")
            tasks = []
            for ip_address in self.leaf_ip_addresses:
                tasks.append(self.client.post(f"http://{ip_address}/healthcheck"))

            responses = await asyncio.gather(*tasks)

            for response in responses:
                response_data = response.json()
                if response_data["status"] == "ok":
                    self.log(f"Successfully connected to leaf at {ip_address}")
            self.log("------------- Healthcheck completed -------------")
                
            self.log("------------- Leaf pipeline creation started -------------")
            # Send a /create_pipeline request to each leaf service and store the pipeline ID
            tasks = []
            for ip_address in self.leaf_ip_addresses:
                tasks.append(self.client.post(f"http://{ip_address}/create_pipeline", json=self.connections))
            
            responses = await asyncio.gather(*tasks)

            for response in responses:
                response_data = response.json()
                pipeline_id = response_data["pipeline_id"]

                for node in self.pipeline.nodes:
                    if isinstance(node, SenderNode):
                        if f"{node.leaf_host}:{node.leaf_port}" == ip_address:
                            node.get_app().set_leaf_pipeline_id(pipeline_id)

                            for connection in self.connections:
                                if connection["sender_name"] == node.name:
                                    connection["pipeline_id"] = pipeline_id
            self.log("------------- Leaf pipeline creation completed -------------")

            self.log("------------- Obtaining leaf pipeline configuration -------------")
            # obtain pipeline configuration from each leaf service and use it to render the pipeline graph
            tasks = []
            for ip_address in self.leaf_ip_addresses:
                tasks.append(self.client.get(f"http://{ip_address}/get_pipeline_config"))

            responses = await asyncio.gather(*tasks)

            leaf_nodes_models = []
            for response in responses:
                response_data = response.json()
                leaf_nodes_models.extend(response_data["nodes"])

            self.log(leaf_nodes_models)
            
            for node in self.pipeline.nodes:
                node_model = node.model()
                node_model.set_origin_url(f"http://{self.host}:{self.port}")

                if isinstance(node, SenderNode):
                    # set the 'successors' attribute of the sender node to the receiver node
                    for leaf_node_model in leaf_nodes_models:

                        if leaf_node_model["name"] == node.leaf_receiver:
                            node_model.add_successor(leaf_node_model["name"])

            # TODO: convert the leaf_node_model to a NodeModel object and call self.pipeline.model().add_node(node_model)
            for leaf_node_model in leaf_nodes_models:
                node_model = NodeModel(**leaf_node_model)
                #self.pipeline.pipeline_model.add_node(node_model)

            self.log("------------- Leaf pipeline configuration obtained -------------")

        except Exception as e:
            print(f"Failed to connect to leaf at {ip_address} with error: {e}")
            self.logger.error(f"Failed to connect to leaf at {ip_address} with error: {e}")
            
    def run(self):
        original_sigint_handler = signal.getsignal(signal.SIGINT)

        def _kill_webserver(sig, frame):
            print(f"\nCTRL+C Caught!; Killing {self.name} Webservice...")
            self.server.should_exit = True
            self.fastapi_thread.join()
            print(f"Anacostia Webservice {self.name} Killed...")

            print("Killing pipeline...")
            self.pipeline.terminate_nodes()
            print("Pipeline Killed.")

            # register the original default kill handler once the pipeline is killed
            signal.signal(signal.SIGINT, original_sigint_handler)

        # register the kill handler for the webserver
        signal.signal(signal.SIGINT, _kill_webserver)
        self.fastapi_thread.start()

        # Launch the root pipeline
        self.pipeline.launch_nodes()
