from fastapi import Request
from fastapi.responses import HTMLResponse

from anacostia_pipeline.dashboard.subapps.basenode import BaseNodeApp
from ..components.filesystemstore import filesystemstore_home, filesystemstore_table, filesystemstore_viewer



class FilesystemStoreNodeApp(BaseNodeApp):
    def __init__(self, node, use_default_file_renderer: str = True, *args, **kwargs):
        super().__init__(
            node, 
            '<link rel="stylesheet" type="text/css" href="/static/css/filesystemstore.css">',
            use_default_router=False, *args, **kwargs
        )

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
            
            return filesystemstore_table(self.file_entries_endpoint, file_entries)
        
        if use_default_file_renderer:
            @self.get("/retrieve_file", response_class=HTMLResponse)
            async def sample(request: Request, file_id: int):
                artifact_path = self.node.get_artifact(file_id)
                content = self.node.load_artifact(artifact_path)
                return filesystemstore_viewer(content, f"Content of {artifact_path}")