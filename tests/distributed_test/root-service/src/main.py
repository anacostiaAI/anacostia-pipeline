from typing import List
import logging
import time
from fastapi import FastAPI
import uvicorn
from threading import Thread



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
        while i<10:
            print(f"hello from root {i}")
            time.sleep(1)
            i += 1


class Webserver(FastAPI):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pipeline: List[Node] = []

        @self.get('/')
        def welcome():
            for _ in range(5):
                node = Node()
                node.daemon = True
                self.pipeline.append(node)
                node.start()
            return "Root pipeline started"
        
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