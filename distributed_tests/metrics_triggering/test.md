### Test Objective:
Show pipeline can trigger properly upon the metrics in the metadata store surpassing a threshold.

### Pipeline Configuration:
Pipeline in `test.py`:
- Running on https://127.0.0.1:8000
- Nodes:
    - metadata store node running on https://127.0.0.1:8000/metadata_store
    - resource node running on https://127.0.0.1:8000/data_store
    - action node running on https://127.0.0.1:8000/logging_node

`log_metrics.py`:
- `edge_deployment_client` (SQLMetadataStoreClient) that logs metrics to the metadata store node.

### Pipeline Trigger:
Pipeline will pull all metrics being logged prior to a run. 
If the highest metric recorded for that run is greater than 0.4, the pipeline will trigger.