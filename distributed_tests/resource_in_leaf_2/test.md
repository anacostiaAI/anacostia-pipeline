### Test Objective:
Show leaf pipeline can record data entries on its temporary metadata store and then the root pipeline can pull data entries from leaf pipeline's
metadata store and train on previously detected artifacts.

### Pipeline Configuration:
Predecessor pipeline:
- Running on https://127.0.0.1:8000
- Nodes:
    - `metadata_store`
        - Type: `SQLiteMetadataStoreNode`
        - Running on https://127.0.0.1:8000/metadata_store
        - Purpose: to provide a central metadata store between the predecessor and successor pipeline.
        - Database location: `./root-artifacts/input_artifacts/metadata_store/metadata.db`

Successor pipeline:
- Running on https://127.0.0.1:8001
- Nodes: 
    - `leaf_metadata_store`
        - Type: `SQLiteMetadataStoreNode`
        - Purpose: to serve as temporary database to record information about files being created prior to the successor pipeline connecting to the predecessor pipeline.
        - Server running on https://127.0.0.1:8001/metadata_store
        - Database location: `./leaf-artifacts/input_artifacts/metadata_store/metadata.db`
    - `leaf_data_node`
        - Type: `FilesystemStoreNode`
        - Running on https://127.0.0.1:8001/leaf_data_node
        - Storage directory: `./leaf-artifacts/input_artifacts/shakespeare`
        - Purpose: to detect incoming files dumped into the `./leaf-artifacts/input_artifacts/shakespeare` folder and to trigger the pipeline.
    - `shakespeare_eval`
        - Type: `BaseActionNode`
        - Running on https://127.0.0.1:8001/shakespeare_eval
        - Purpose: only here to provide a placeholder.
- Clients:
    - `metadata_store_rpc`
        - Type: `SQLMetadataStoreClient`
        - Running on https://127.0.0.1:8001/metadata_store_rpc/api/client
        - Purpose: to provide an interface with the `metadata_store` on the predecessor pipeline.

### Test Setup:
Files will be created and dumped into the `./leaf-artifacts/input_artifacts/shakespeare` folder (via `create_files_1.py`). 
`leaf_data_node` will monitor the folder and record entries into the `leaf_metadata_store` upon new files being dumped into the folder. 
The predecessor pipeline will then start running, `metadata_store` will pull data entries on the files created by `create_files_1.py`,
and then enter the data entries into the SQLite database in `metadata_store`.

### Pipeline Trigger:
`create_files_2.py` then creates files and dumps it into the `./leaf-artifacts/input_artifacts/shakespeare` folder as well. 
`leaf_data_node` will then trigger the pipeline on new files being created by `create_files_2.py`.

### Pipeline operation:
Files created by `create_files_1.py` and `create_files_2.py` are then used for the first run.