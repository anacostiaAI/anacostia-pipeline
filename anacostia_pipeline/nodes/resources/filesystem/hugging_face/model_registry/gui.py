from fastapi import Request
from fastapi.responses import HTMLResponse, StreamingResponse
import asyncio
from typing import List, Dict, Tuple, Union
import markdown
import yaml

from anacostia_pipeline.nodes.gui import BaseGUI
from anacostia_pipeline.nodes.resources.filesystem.hugging_face.model_registry.fragments import model_registry_home, model_entry_card, model_card_modal
from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.nodes.metadata.api import BaseMetadataStoreClient
from anacostia_pipeline.utils.sse import format_html_for_sse



class ModelRegistryGUI(BaseGUI):
    def __init__(
        self, node, host: str, port: int, 
        metadata_store: BaseMetadataStoreNode = None, 
        metadata_store_client: BaseMetadataStoreClient = None, 
        *args, **kwargs
    ):
        super().__init__(node, host=host, port=port, use_default_router=False, *args, **kwargs)

        if metadata_store is None and metadata_store_client is None:
            raise ValueError("Either metadata_store or metadata_store_rpc must be provided")

        self.metadata_store = metadata_store
        self.metadata_store_client = metadata_store_client

        self.event_source = f"{self.get_gui_url()}/table_update_events"
        self.event_name = "TableUpdate"

        self.displayed_file_entries = None

        def format_file_entries(file_entries: Union[List[Dict], Dict]) -> Union[List[Dict], Dict]:
            model_entries = [entry for entry in file_entries if entry["location"].endswith(".md") is False]

            for model_entry in model_entries:
                model_path = model_entry['location']
                tags = self.metadata_store.get_artifact_tags(location=model_path)
                for tag in tags:
                    if "model_card_path" in tag.keys():
                        model_card_path = tag["model_card_path"]
                        model_entry['modal_open_endpoint'] = f"{self.get_gui_url()}/modal/?action=open&card_path={model_card_path}"
                        model_entry['location'] = f"{self.node.resource_path}/{model_path}"
                        break

            return model_entries
        
        @self.get("/home", response_class=HTMLResponse)
        async def endpoint(request: Request):
            if self.metadata_store is not None:
                file_entries = self.metadata_store.get_entries(resource_node_name=self.node.name)
            else:
                if self.metadata_store_client is not None:
                    file_entries = await self.metadata_store_client.get_entries(resource_node_name=self.node.name)

            self.displayed_file_entries = file_entries
            file_entries.reverse()
            model_entries = format_file_entries(file_entries)

            return model_registry_home(
                update_endpoint=f"{self.get_gui_url()}/update_home_page",
                model_entries = model_entries
            ) 
        
        @self.get("/update_home_page", response_class=HTMLResponse)
        async def update_home_page(request: Request):
            if self.metadata_store is not None:
                file_entries = self.metadata_store.get_entries(resource_node_name=self.node.name)
            else:
                if self.metadata_store_client is not None:
                    file_entries = await self.metadata_store_client.get_entries(resource_node_name=self.node.name)

            self.displayed_file_entries = file_entries
            file_entries.reverse()
            model_entries = format_file_entries(file_entries)

            model_entries_str = "\n".join([
                model_entry_card(model_entry) for model_entry in model_entries
            ]) 

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
                    modal_close_endpoint=f"{self.get_gui_url()}/modal/?action=close", 
                    markdown_html_str=html
                )

            elif action == "close":
                return ""
