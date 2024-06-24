from typing import List
import logging
import time
from fastapi import FastAPI
import uvicorn
from threading import Thread
import requests



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



class Node(Thread):
    def run(self) -> None:
        i = 0
        while True:
            print(f"hello from root {i}", flush=True)
            time.sleep(1)
            i += 1


class Webserver(FastAPI):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pipeline: List[Node] = []

        @self.get('/')
        def welcome():
            for _ in range(2):
                node = Node()
                node.daemon = True
                self.pipeline.append(node)
                node.start()
            return "Root pipeline started"
        
        @self.get("/forward_signal")
        def forward_signal_handler():
            response = requests.get(url="http://leaf-pipeline:8080/forward_signal")
            print(response.text, flush=True)
            return response.text

        @self.get("/backward_signal")
        def backward_signal_handler():
            return "response from root"

        @self.get("/stop")
        def stop():
            for node in self.pipeline:
                node.join()
            return "Root pipeline stopped"


def run_background_webserver(**kwargs):
    app = Webserver()
    config = uvicorn.Config(app, host="0.0.0.0", port=8000)
    server = uvicorn.Server(config)
    fastapi_thread = Thread(target=server.run)
    fastapi_thread.start()


if __name__ == "__main__":
    run_background_webserver()