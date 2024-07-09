from typing import List
import logging
import time
from fastapi import FastAPI, status
import uvicorn
import threading
import requests
from anacostia_pipeline.engine.pipeline import Pipeline



root_test_path = "/testing_artifacts"

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


class RootWebserver(FastAPI):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pipeline: List[Node] = []

        @self.get("/forward_signal")
        def forward_signal_handler():
            response = requests.get(url="http://leaf-pipeline:8080/forward_signal")
            print(response.text, flush=True)
            return response.text

        @self.get("/backward_signal")
        def backward_signal_handler():
            return "response from root"
        
        @self.get("/is_started")
        def healthcheck():
            return "good"

        @self.get("/create")
        def stop():
            for _ in range(2):
                node = Node()
                node.daemon = True
                self.pipeline.append(node)
            logger.info("Root pipeline created...")

            response = requests.post(url="http://leaf-pipeline:8080/create")
            if response.status_code == status.HTTP_201_CREATED:
                return "Root and leaf pipelines created..."
            else:
                return "pipeline creation failed"

        @self.get('/start', status_code=status.HTTP_200_OK)
        def start():
            for node in self.pipeline:
                node.start()
            logger.info("Root pipeline started running...")

            response = requests.post(url="http://leaf-pipeline:8080/start")
            if response.status_code == status.HTTP_200_OK:
                return "Root and leaf pipelines started running..."
            else:
                return "pipeline start failed"

        @self.get('/shutdown')
        def shutdown():
            for node in self.pipeline:
                node.pause_event.set()
                node.shutdown_event.set()
                node.join()
            logger.info("Root pipeline shutdown...")
            
            response = requests.post(url="http://leaf-pipeline:8080/shutdown")
            if response.status_code == status.HTTP_200_OK:
                return "Root and leaf pipelines shutdown..."
            else:
                return "pipeline shutdown failed"
        
        @self.get("/pause")
        def pause():
            for node in self.pipeline:
                node.pause_event.clear()
            logger.info("Root pipeline paused...")
            
            response = requests.post(url="http://leaf-pipeline:8080/pause")
            if response.status_code == status.HTTP_200_OK:
                return "Root and leaf pipelines paused..."
            else:
                return "pipeline pause failed"

        @self.get("/resume")
        def resume():
            for node in self.pipeline:
                node.pause_event.set()
            logger.info("Root pipeline resumed running...")
            
            response = requests.post(url="http://leaf-pipeline:8080/resume")
            if response.status_code == status.HTTP_200_OK:
                return "Root and leaf pipelines resumed running..."
            else:
                return "pipeline resume failed"
        

def run_background_webserver(**kwargs):
    app = RootWebserver()
    config = uvicorn.Config(app, host="0.0.0.0", port=8000)
    server = uvicorn.Server(config)
    fastapi_thread = threading.Thread(target=server.run)
    fastapi_thread.start()


if __name__ == "__main__":
    run_background_webserver()