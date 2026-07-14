from fastapi import Request
from fastapi.responses import HTMLResponse
from fastapi.routing import APIRoute
from typing import List, Dict, Union
import markdown
import yaml
from logging import Logger

from anacostia_pipeline.nodes.gui import BaseGUI
from anacostia_pipeline.nodes.resources.filesystem.hugging_face.model_registry.fragments import model_entry_card, model_card_modal
from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.nodes.metadata.api import BaseMetadataStoreClient
from anacostia_pipeline.nodes.resources.filesystem.hugging_face.model_registry.node import HuggingFaceModelRegistryNode

from fragments import ude_head_template
from model_registry_fragments import ude_model_registry_home



class UDEModelRegistryGUI(BaseGUI):
    def __init__(
        self, node, host: str, port: int, 
        metadata_store: BaseMetadataStoreNode = None, 
        metadata_store_client: BaseMetadataStoreClient = None, 
        ssl_keyfile: str = None, ssl_certfile: str = None, ssl_ca_certs: str = None,
        *args, **kwargs
    ):
        super().__init__(node, host=host, port=port, ssl_keyfile=ssl_keyfile, ssl_certfile=ssl_certfile, ssl_ca_certs=ssl_ca_certs, *args, **kwargs)

        if metadata_store is None and metadata_store_client is None:
            raise ValueError("Either metadata_store or metadata_store_rpc must be provided")

        self.metadata_store = metadata_store
        self.metadata_store_client = metadata_store_client

        def render_file_entries(file_entries: Union[List[Dict], Dict]) -> Union[List[Dict], Dict]:
            model_entries = [entry for entry in file_entries if entry["location"].endswith(".md") is False]

            entries_to_render = []

            for model_entry in model_entries:
                model_path = model_entry['location']
                tags = self.metadata_store.get_artifact_tags(location=model_path)
                
                if any("model_card_path" in tag.keys() for tag in tags) is False:
                    entries_to_render.append(model_entry_card(model_entry))
                else:
                    for tag in tags:
                        model_card_path = tag["model_card_path"]
                        entries_to_render.append(
                            model_entry_card(
                                model_entry, 
                                modal_open_endpoint=f"{self.get_node_prefix(remove_beginning_slash=True)}/modal/?action=open&card_path={model_card_path}"
                            )
                        )
                
            return entries_to_render
        
        @self.get("/home", response_class=HTMLResponse)
        async def endpoint(request: Request):
            if self.metadata_store is not None:
                file_entries = self.metadata_store.get_entries(resource_node_name=self.node.name)
            else:
                if self.metadata_store_client is not None:
                    file_entries = await self.metadata_store_client.get_entries(resource_node_name=self.node.name)

            file_entries.reverse()
            model_entries = render_file_entries(file_entries)

            return self.home(
                update_endpoint=f"{self.get_node_prefix(remove_beginning_slash=True)}/update_home_page",
                model_entries = model_entries
            )
        
        @self.get("/update_home_page", response_class=HTMLResponse)
        async def update_home_page(request: Request):
            if self.metadata_store is not None:
                file_entries = self.metadata_store.get_entries(resource_node_name=self.node.name)
            else:
                if self.metadata_store_client is not None:
                    file_entries = await self.metadata_store_client.get_entries(resource_node_name=self.node.name)

            file_entries.reverse()
            model_entries = render_file_entries(file_entries)
            model_entries_str = "\n".join(model_entries) 
            return model_entries_str
        
        @self.get("/modal/", response_class=HTMLResponse)
        async def modal(action: str, card_path: str = None):
            if action == "open":
                # Load the markdown file
                card_path = f"{self.node.resource_path}/{card_path}"
                with open(card_path, "r") as f:
                    content = f.read()

                # Separate YAML frontmatter and body
                if content.startswith("---"):
                    _, yaml_block, body = content.split("---", 2)
                    metadata = yaml.safe_load(yaml_block)
                else:
                    body = content

                # Render markdown (including embedded HTML)
                html = markdown.markdown(body, extensions=['extra', 'toc', 'nl2br', "fenced_code"])
                return model_card_modal(
                    modal_close_endpoint=f"{self.get_node_prefix(remove_beginning_slash=True)}/modal/?action=close", 
                    markdown_html_str=html
                )

            elif action == "close":
                return ""

    def home(self, update_endpoint: str, model_entries: List[str]):
        return ude_model_registry_home(
            head_template=ude_head_template(
                '''
                <!-- Load MathJax config first -->
                <script src="static/js/src/mathjax-config.js"></script>
                <script type="text/javascript" id="MathJax-script" async src="static/js/third_party/mathjax.js"></script>

                <!-- CSS for model registry home page -->
                <link rel="stylesheet" href="static/css/styles/model_registry.css">

                <!-- CSS for markdown modal -->
                <link rel="stylesheet" href="static/css/styles/markdown.css">
                '''
            ),
            update_endpoint=update_endpoint,
            model_entries=model_entries
        )

    def get_node_prefix(self, remove_beginning_slash: bool = False):
        if remove_beginning_slash:
            return f"{self.node.name}/hypermedia"
        else:
            return f"/{self.node.name}/hypermedia"

    def get_home_endpoint(self):
        if "/home" in [route.path for route in self.routes if isinstance(route, APIRoute)]:
            return f"{self.node.name}/hypermedia/home"
        else:
            return ''


class UDEModelRegistryNode(HuggingFaceModelRegistryNode):
    def __init__(
        self, 
        name: str, 
        resource_path: str, 
        metadata_store: BaseMetadataStoreNode = None, 
        metadata_store_client: BaseMetadataStoreClient = None, 
        hash_chunk_size: int = 1_048_576, 
        max_old_samples: int = None, 
        remote_predecessors: List[str] = None, 
        remote_successors: List[str] = None, 
        client_url: str = None, 
        wait_for_connection: bool = False, 
        loggers: Union[Logger, List[Logger]] = None, 
        monitoring: bool = True
    ):
        super().__init__(
            name=name, 
            resource_path=resource_path, 
            metadata_store=metadata_store, 
            metadata_store_client=metadata_store_client, 
            hash_chunk_size=hash_chunk_size,
            max_old_samples=max_old_samples, 
            remote_predecessors=remote_predecessors, 
            remote_successors=remote_successors, 
            client_url=client_url, 
            wait_for_connection=wait_for_connection, 
            loggers=loggers, 
            monitoring=monitoring
        )

    def setup_node_GUI(self, host: str, port: int, ssl_keyfile: str = None, ssl_certfile: str = None, ssl_ca_certs: str = None):
        self.gui = UDEModelRegistryGUI(
            node=self, host=host, port=port, 
            metadata_store=self.metadata_store, 
            metadata_store_client=self.metadata_store_client,
            ssl_keyfile=ssl_keyfile, ssl_certfile=ssl_certfile, ssl_ca_certs=ssl_ca_certs
        )
        return self.gui