import os
import sys

from fastapi import Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from anacostia_pipeline.dashboard.subapps.basenode import BaseNodeApp
from ..components.filesystemstore import filesystemstore_home, filesystemstore_table



class FilesystemStoreNodeRouter(BaseNodeApp):
    def __init__(self, node, use_default_file_renderer: str = True, *args, **kwargs):
        super().__init__(
            node, 
            '<link rel="stylesheet" type="text/css" href="/static/css/filesystemstore.css">',
            use_default_router=False, *args, **kwargs
        )

        PACKAGE_NAME = "anacostia_pipeline"
        PACKAGE_DIR = os.path.dirname(sys.modules[PACKAGE_NAME].__file__)
        self.templates_dir = os.path.join(PACKAGE_DIR, "templates")
        self.templates = Jinja2Templates(directory=self.templates_dir)

        self.file_entries_endpoint = f"{self.get_prefix()}/file_entries"

        @self.get("/home", response_class=HTMLResponse)
        async def endpoint(request: Request):
            file_entries = self.node.metadata_store.get_entries(node, "all")
            file_entries = [sample.as_dict() for sample in file_entries]
            for file_entry in file_entries:
                file_entry['created_at'] = file_entry['created_at'].strftime("%m/%d/%Y, %H:%M:%S")
                file_entry["file_display_endpoint"] = f"{self.get_prefix()}/retrieve_file?file_id={file_entry['id']}"
                if file_entry['end_time'] is not None:
                    file_entry['end_time'] = file_entry['end_time'].strftime("%m/%d/%Y, %H:%M:%S")
            
            """
            response = self.templates.TemplateResponse(
                "filesystemstore/filesystemstore.html", 
                {   
                    "request": request,
                    "node": self.node.model(), 
                    "status_endpoint": self.get_status_endpoint(),
                    "work_endpoint": self.get_work_endpoint(),
                    "header_bar_endpoint": self.get_header_bar_endpoint(),
                    "file_entries": file_entries,
                    "file_entries_endpoint": self.file_entries_endpoint 
                }
            )
            return response
            """
            return filesystemstore_home(
                header_bar_endpoint = self.get_header_bar_endpoint(),
                file_entries_endpoint = self.file_entries_endpoint, 
                file_entries = file_entries
            ) 

        @self.get("/file_entries", response_class=HTMLResponse)
        async def samples(request: Request):
            file_entries = self.node.metadata_store.get_entries(node, "all")
            file_entries = [sample.as_dict() for sample in file_entries]
            for file_entry in file_entries:
                file_entry['created_at'] = file_entry['created_at'].strftime("%m/%d/%Y, %H:%M:%S")
                file_entry["file_display_endpoint"] = f"{self.get_prefix()}/retrieve_file?file_id={file_entry['id']}"
                if file_entry['end_time'] is not None:
                    file_entry['end_time'] = file_entry['end_time'].strftime("%m/%d/%Y, %H:%M:%S")
            
            """
            response = self.templates.TemplateResponse(
                "filesystemstore/filesystemstore_files.html", 
                {"request": request, "file_entries": file_entries, "file_entries_endpoint": self.file_entries_endpoint }
            )
            return response
            """
            return filesystemstore_table(self.file_entries_endpoint, file_entries)
        
        if use_default_file_renderer:
            @self.get("/retrieve_file", response_class=HTMLResponse)
            async def sample(request: Request, file_id: int):
                artifact_path = self.node.get_artifact(file_id)
                content = self.node.load_artifact(artifact_path)

                response = self.templates.TemplateResponse(
                    "filesystemstore/filesystemstore_file_viewer.html", 
                    {"request": request, "content": content, "box_header": f"Content of {artifact_path}"}
                )
                return response