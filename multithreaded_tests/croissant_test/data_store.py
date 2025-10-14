import os
import json
from datetime import datetime

import mlcroissant as mlc

from anacostia_pipeline.nodes.resources.filesystem.croissant.node import DatasetRegistryNode



class CustomDatasetRegistryNode(DatasetRegistryNode):
    def __init__(
        self, 
        name, 
        resource_path, 
        metadata_store = None, 
        metadata_store_client = None, 
        hash_chunk_size = 1048576, 
        max_old_samples = None, 
        remote_predecessors = None, 
        remote_successors = None, 
        client_url = None, 
        wait_for_connection = False, 
        loggers = None
    ):
        super().__init__(
            name, 
            resource_path, 
            metadata_store, 
            metadata_store_client, 
            hash_chunk_size, 
            max_old_samples, 
            remote_predecessors, 
            remote_successors, 
            client_url, 
            wait_for_connection, 
            loggers
        )
    
    def save_data_card(self):

        datasets_produced = self.list_artifacts(state="produced")

        # 1) One FileObject per concrete file (good for checksums)
        distribution = []
        for filepath in datasets_produced:
            fullpath = os.path.join(self.resource_path, filepath)

            distribution.append(
                mlc.FileObject(
                    name=os.path.basename(fullpath),
                    content_url=filepath,          # relative path is portable
                    encoding_formats=["text/plain"],
                    sha256=self.hash_file(fullpath)
                )
            )

        # 2) A FileSet that groups all *.txt files into one logical resource (lets RecordSet refer to them as a single source)
        text_files = mlc.FileSet(
            id="text-files",
            name="text-files",
            includes=f"{self.resource_path}/*.txt",
            encoding_formats=["text/plain"]
        )

        # 3) RecordSet: each record = one file; expose filename and content
        record_set = mlc.RecordSet(
            name="examples",
            description="Each record corresponds to one text file.",
            key="hash",
            fields=[
                mlc.Field(
                    name="filename",
                    data_types=mlc.DataType.TEXT,
                    source=mlc.Source(
                        file_set="text-files",             # refer to the FileSet by name
                        extract=mlc.Extract(file_property="filename"),
                    ),
                ),
                mlc.Field(
                    name="content",
                    data_types=mlc.DataType.TEXT,
                    source=mlc.Source(
                        file_set="text-files",             # refer to the FileSet by name
                        extract=mlc.Extract(file_property="content"),
                    ),
                ),
            ],
        )

        # 4) Top-level Metadata (schema.org Dataset), then serialize to JSON-LD
        metadata = mlc.Metadata(
            name="Test Text Dataset",
            description="A simple Croissant dataset containing local text files for testing.",
            license="https://creativecommons.org/licenses/by/4.0/",
            url="https://example.com/dataset/test-text",
            conforms_to="http://mlcommons.org/croissant/1.0",
            distribution=[*distribution, text_files],
            record_sets=[record_set],
        )
        metadata.date_published = datetime.now().strftime("%Y-%m-%d")

        # 5) Save to JSON-LD file in the data store
        run_id = self.run_id
        with self.save_artifact(filepath=f"data_card_{run_id}.json") as fullpath:
            with open(fullpath, 'w', encoding='utf-8') as json_file:
                content = metadata.to_json()
                json.dump(content, json_file, indent=4)

        self.log(f"Data card saved with files: {datasets_produced}", level="INFO")