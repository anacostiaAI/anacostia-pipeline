from typing import List
import logging
import time
from fastapi import FastAPI
import uvicorn
import threading
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



class Node(threading.Thread):
    def __init__(self) -> None:
        super().__init__()

    def run(self) -> None:
        i = 0
        while True:
            if not self.stop_event.is_set():
                print(f"hello from leaf {i}", flush=True)
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
            return "Leaf pipeline started"
        
        @self.get("/forward_signal")
        def forward_signal_handler():
            return "response from leaf"
        
        @self.get("/backward_signal")
        def backward_signal_handler():
            response = requests.get(url="http://root-pipeline:8000/backward_signal")
            print(response.text, flush=True)
            return response.text

        @self.get("/stop")
        def stop():
            for node in self.pipeline:
                node.join()
            return "Leaf pipeline stopped"


def run_background_webserver(**kwargs):
    app = Webserver()
    config = uvicorn.Config(app, host="0.0.0.0", port=8080)
    server = uvicorn.Server(config)
    fastapi_thread = threading.Thread(target=server.run)
    fastapi_thread.start()


if __name__ == "__main__":
    run_background_webserver()