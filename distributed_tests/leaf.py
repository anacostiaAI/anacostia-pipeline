from typing import List
import logging
import time
from fastapi import FastAPI, status
import uvicorn
import threading
import httpx
import signal
from contextlib import asynccontextmanager
import argparse

from anacostia_pipeline.engine.pipeline import Pipeline



root_test_path = "./testing_artifacts"

log_path = f"{root_test_path}/anacostia.log"
logging.basicConfig(
    level=logging.DEBUG,
    format='LEAF %(asctime)s - %(levelname)s - %(message)s',
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

            print(f"hello from leaf {i}", flush=True)
            time.sleep(1)
            i += 1


class LeafWebserver(FastAPI):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.pipeline: List[Node] = []

        self.client: httpx.AsyncClient = None

        @self.get("/")
        async def main_page():
            return "hello from leaf"

        @self.get("/forward_signal")
        def forward_signal_handler():
            return "response from leaf"
        
        @self.get("/backward_signal")
        async def backward_signal_handler():
            response = await self.client.get(url="http://root-pipeline:8000/backward_signal")
            print(response.text, flush=True)
            return response.text
        
        @self.get("/is_started")
        def healthcheck():
            return "good"
         
        """
        @self.get('/create', status_code=status.HTTP_201_CREATED)
        def create():
            return "0.0.0.0:8000"
        """

        @self.post('/create', status_code=status.HTTP_201_CREATED)
        def create():
            for _ in range(2):
                node = Node()
                node.daemon = True
                self.pipeline.append(node)
            logger.info("Leaf pipeline created")

        @self.post('/start', status_code=status.HTTP_200_OK)
        def start():
            for node in self.pipeline:
                node.start()
            logger.info("Leaf pipeline started")
        
        """
        @self.get('/shutdown', status_code=status.HTTP_200_OK)
        def shutdown():
            return "0.0.0.0:8000"
        """
        
        @self.post('/shutdown', status_code=status.HTTP_200_OK)
        def shutdown():
            for node in self.pipeline:
                node.pause_event.set()
                node.shutdown_event.set()
                node.join()
            logger.info("Leaf pipeline shutdown")
        
        @self.post("/pause", status_code=status.HTTP_200_OK)
        def pause():
            for node in self.pipeline:
                node.pause_event.clear()
            logger.info("Leaf pipeline pause")

        @self.post("/resume", status_code=status.HTTP_200_OK)
        def resume():
            for node in self.pipeline:
                node.pause_event.set()
            logger.info("Leaf pipeline resume")



@asynccontextmanager
async def life(app: LeafWebserver):
    app.client = httpx.AsyncClient()
    yield
    await app.client.aclose()



def run_background_webserver(**kwargs):
    app = LeafWebserver(lifespan=life)
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