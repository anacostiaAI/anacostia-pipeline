from typing import List
import logging
import time
from fastapi import FastAPI, status
import httpx
import uvicorn
import threading
import asyncio
from contextlib import asynccontextmanager
import signal
import argparse

from anacostia_pipeline.engine.pipeline import Pipeline



root_test_path = "./testing_artifacts"

log_path = f"{root_test_path}/anacostia.log"
logging.basicConfig(
    level=logging.DEBUG,
    format='ROOT %(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='a'
)
logger = logging.getLogger(__name__)



class Node(threading.Thread):
    def __init__(self) -> None:
        super().__init__()
        self.shutdown_event = threading.Event()
        self.pause_event = threading.Event()
        self.pause_event.set()

    def run(self) -> None:
        print("node started running", flush=True)
        i = 0
        while True:
            self.pause_event.wait()
            if self.shutdown_event.is_set() is True:
                print("node shutting down", flush=True)
                break

            print(f"hello from root {i}", flush=True)
            time.sleep(1)
            i += 1


# Note: in the future, this RootWebserver class will be renamed to AnacostiaWebserver
# each instance of the AnacostiaWebserver class will be responsible for handling all the web traffic for one copy of a subgraph.
# This means that each instance of the AnacostiaWebserver class will be bound to a single port.
class RootWebserver(FastAPI):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pipeline: List[Node] = []

        self.client: httpx.AsyncClient = None

        @self.get("/")
        async def main_page():
            return "hello from root"

        @self.get("/forward_signal")
        async def forward_signal_handler():
            response = await self.client.get(url="http://leaf-pipeline:8080/forward_signal")
            print(response.text, flush=True)
            return response.text

        @self.get("/backward_signal")
        def backward_signal_handler():
            return "response from root"
        
        @self.get("/is_started")
        def healthcheck():
            return "good"

        @self.get("/create")
        async def stop():
            for _ in range(2):
                node = Node()
                node.daemon = True
                self.pipeline.append(node)
            logger.info("Root pipeline created...")

            response = await self.client.post(url="http://leaf-pipeline:8080/create")
            if response.status_code == status.HTTP_201_CREATED:
                return "Root and leaf pipelines created..."
            else:
                return "pipeline creation failed"

        @self.get('/start', status_code=status.HTTP_200_OK)
        async def start():
            for node in self.pipeline:
                node.start()
            logger.info("Root pipeline started running...")

            response = await self.client.post(url="http://leaf-pipeline:8080/start")
            if response.status_code == status.HTTP_200_OK:
                return "Root and leaf pipelines started running..."
            else:
                return "pipeline start failed"

        @self.get('/shutdown')
        async def shutdown():
            for node in self.pipeline:
                node.pause_event.set()
                node.shutdown_event.set()
                node.join()
            logger.info("Root pipeline shutdown...")
            
            response = await self.client.post(url="http://leaf-pipeline:8080/shutdown")
            if response.status_code == status.HTTP_200_OK:
                return "Root and leaf pipelines shutdown..."
            else:
                return "pipeline shutdown failed"
        
        @self.get("/pause")
        async def pause():
            for node in self.pipeline:
                node.pause_event.clear()
            logger.info("Root pipeline paused...")
            
            response = await self.client.post(url="http://leaf-pipeline:8080/pause")
            if response.status_code == status.HTTP_200_OK:
                return "Root and leaf pipelines paused..."
            else:
                return "pipeline pause failed"

        @self.get("/resume")
        async def resume():
            for node in self.pipeline:
                node.pause_event.set()
            logger.info("Root pipeline resumed running...")
            
            response = await self.client.post(url="http://leaf-pipeline:8080/resume")
            if response.status_code == status.HTTP_200_OK:
                return "Root and leaf pipelines resumed running..."
            else:
                return "pipeline resume failed"
        


class AnacostiaService(FastAPI):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        
        self.client: httpx.AsyncClient = None

        # Note: in the future, Anacostia service will pull the origins from connector nodes in the pipeline
        self.leaf_origins = ["http://leaf-pipeline:8080"]

        @self.get("/is_started")
        def healthcheck():
            return "good"
    

@asynccontextmanager
async def lifespan(app: AnacostiaService):
    app.client = httpx.AsyncClient()

    # on start up, ping all leaf pipelines to ensure proper connection
    # Note: all leaf pipelines must be online prior to root pipeline starts running
    responses = await asyncio.gather(*[app.client.get(f"{url}/create") for url in app.leaf_origins])
    for response in responses:
        print(f"leaf created {response.text}")

    yield

    # on shutdown, ping all leaf pipelines to shutoff and destroy pipeline instances
    responses = await asyncio.gather(*[app.client.get(f"{url}/shutdown") for url in app.leaf_origins])
    for response in responses:
        print(f"leaf shutdown {response.text}")




# Note: in the future, run_service will work like so:
# 1. take in a pipeline as an argument
# 2. automatically scan the pipeline
# 3. finds connection nodes that work across servers
# 4. retrieve the host+port of the leaf pipeline service
# 5. make the connection with the leaf pipeline service
# 6. spin up the webserver and the pipeline
def run_service(host: str = "0.0.0.0", port: int = 8000):
    app = AnacostiaService(lifespan=lifespan)
    config = uvicorn.Config(app, host=host, port=port)
    server = uvicorn.Server(config)
    fastapi_thread = threading.Thread(target=server.run)

    def signal_handler(sig, frame):
        # Handle SIGTERM here
        print('SIGTERM received, performing cleanup...')
        server.should_exit = True
        fastapi_thread.join()

    # Register signal handler for SIGTERM (this is done for shutting down docker containers via docker stop)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Register signal handler for SIGINT (this is done for shutting down using Ctrl+C)
    signal.signal(signal.SIGINT, signal_handler)

    fastapi_thread.start()



@asynccontextmanager
async def life(app: RootWebserver):
    app.client = httpx.AsyncClient()
    yield
    await app.client.aclose()


# Note: in the future, this function will be a part of the AnacostiaService class 
# and it will be responsible for spinning up multiple instances of the AnacostiaWebserver class
# This means that each instance of the AnacostiaService class will be bound to a single ip address.
def run_background_webserver(**kwargs):
    app = RootWebserver(lifespan=life)
    config = uvicorn.Config(app, **kwargs)
    server = uvicorn.Server(config)
    fastapi_thread = threading.Thread(target=server.run)

    def signal_handler(sig, frame):
        logger.debug(f'{sig} received, performing cleanup for root...')
        server.should_exit = True
        fastapi_thread.join()

    # Register signal handler for SIGTERM (this is done for shutting down via test.sh)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Register signal handler for SIGINT (this is done for shutting down via Ctrl+C from the command line)
    # signal.signal(signal.SIGINT, signal_handler)

    fastapi_thread.start()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('host', type=str)
    parser.add_argument('port', type=int)
    args = parser.parse_args()

    run_background_webserver(host=args.host, port=args.port)