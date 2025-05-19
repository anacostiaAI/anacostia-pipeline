from typing import List, Dict
from anacostia_pipeline.pipelines.fragments import head_template



newline = "\n"

def sqlmetadatastore_runs_table(runs: List[Dict[str, str]], runs_endpoint: str):
    return f"""
        <table class="table is-bordered is-striped is-hoverable"
            hx-get="{ runs_endpoint }" hx-trigger="every 1s" hx-swap="outerHTML" hx-target="this">
            <thead>
                <tr>
                    <th>Run ID</th>
                    <th>Start Time</th>
                    <th>Run Hash</th>
                </tr>
            </thead>
            <tbody>
                {
                    newline.join([
                        f'''
                        <tr>
                            <th>{ run["run_id"] }</th>
                            <td>{ run["start_time"] }</td>
                            <td>{ run["hash"] }</td>
                        </tr>
                        ''' for run in runs
                    ])
                }
            </tbody>
        </table>
    """


def sqlmetadatastore_home(data_options: Dict[str, str], runs: List[Dict[str, str]]):
    return f"""
        {head_template(
            '''
            <!-- Bulma CSS --> 
            <link rel="stylesheet" href="/static/css/third_party/bulma.css">

            <!-- Note: the /static directory is not mounted here, but in the main webserver -->
            <link rel="stylesheet" type="text/css" href="/static/css/styles/sqlmetadatastore.css">
            '''
        )}

        <div id="data_type_menu" class="dropdown is-hoverable">
            <div class="dropdown-trigger">
                <button class="button" aria-haspopup="true" aria-controls="dropdown-menu3">
                    <span class="icon-text">
                        <span>View Other Tables</span>
                        <span class="icon">▼</span>
                    </span>
                </button>
            </div>
            <div class="dropdown-menu" id="dropdown-menu3" role="menu">
                <div class="dropdown-content">
                    {
                        newline.join([
                            f'''
                            <a href="{ endpoint }" class="dropdown-item" 
                                hx-get="{ endpoint }" hx-target="#table_container" hx-swap="innerHTML" hx-trigger="click">
                                { data_type }
                            </a>
                            ''' for data_type, endpoint in data_options.items()
                        ])
                    }
                </div>
            </div>
        </div>
        <div id="table_container" class="container">
            {sqlmetadatastore_runs_table(runs, data_options["runs"])}
        </div>
    """


def sqlmetadatastore_samples_table(samples: List[Dict[str, str]], samples_endpoint: str):
    return f"""
        <table class="table is-bordered is-striped is-hoverable"
            hx-get="{ samples_endpoint }" hx-trigger="every 1s" hx-swap="outerHTML" hx-target="this">
            <thead>
                <tr>
                    <th>Sample ID</th>
                    <th>Run ID</th>
                    <th>Node Name</th>
                    <th>Created At</th>
                    <th>Location</th>
                    <th>State</th>
                    <th>Hash</th>
                    <th>Hash Algorithm</th>
                </tr>
            </thead>
            <tbody>
                {
                    newline.join([
                        f'''
                        <tr>
                            <th>{ sample["id"] }</th>
                            <td>{ sample["run_id"] }</td>
                            <td>{ sample["node_name"] }</td>
                            <td>{ sample["created_at"] }</td>
                            <td>{ sample["location"] }</td>
                            <td>{ sample["state"] }</td>
                            <td>{ sample["hash"] }</td>
                            <td>{ sample["hash_algorithm"] }</td>
                        </tr>
                        ''' for sample in samples
                    ])
                }
            </tbody>
        </table>
    """


def sqlmetadatastore_metrics_table(metrics: List[Dict[str, str]], metrics_endpoint: str):
    return f"""
        <table class="table is-bordered is-striped is-hoverable"
            hx-get="{ metrics_endpoint }" hx-trigger="every 1s" hx-swap="outerHTML" hx-target="this">
            <thead>
                <tr>
                    <th>Entry ID</th>
                    <th>Run ID</th>
                    <th>Node Name</th>
                    <th>Metric</th>
                    <th>Value</th>
                </tr>
            </thead>
            <tbody>
                {
                    newline.join([
                        f'''
                        <tr>
                            <th>{ metric["id"] }</th>
                            <td>{ metric["run_id"] }</td>
                            <td>{ metric["node_name"] }</td>
                            <td>{ metric["metric_name"] }</td>
                            <td>{ metric["metric_value"] }</td>
                        </tr>
                        ''' for metric in metrics
                    ])
                }
            </tbody>
        </table> 
    """


def sqlmetadatastore_params_table(params: List[Dict[str, str]], params_endpoint: str):
    return f"""
        <table class="table is-bordered is-striped is-hoverable"
            hx-get="{ params_endpoint }" hx-trigger="every 1s" hx-swap="outerHTML" hx-target="this">
            <thead>
                <tr>
                    <th>Entry ID</th>
                    <th>Run ID</th>
                    <th>Node Name</th>
                    <th>Param Name</th>
                    <th>Value</th>
                </tr>
            </thead>
            <tbody>
                {
                    newline.join([
                        f'''
                        <tr>
                            <th>{ param["id"] }</th>
                            <td>{ param["run_id"] }</td>
                            <td>{ param["node_name"] }</td>
                            <td>{ param["param_name"] }</td>
                            <td>{ param["param_value"] }</td>
                        </tr>
                        ''' for param in params
                    ])
                }
            </tbody>
        </table>
    """


def sqlmetadatastore_tags_table(tags: List[Dict[str, str]], tags_endpoint: str):
    return f"""
        <table class="table is-bordered is-striped is-hoverable"
            hx-get="{ tags_endpoint }" hx-trigger="every 1s" hx-swap="outerHTML" hx-target="this">
            <thead>
                <tr>
                    <th>Entry ID</th>
                    <th>Run ID</th>
                    <th>Node Name</th>
                    <th>Tag Name</th>
                    <th>Value</th>
                </tr>
            </thead>
            <tbody>
                {
                    newline.join([
                        f'''
                        <tr>
                            <th>{ tag["id"] }</th>
                            <td>{ tag["run_id"] }</td>
                            <td>{ tag["node_name"] }</td>
                            <td>{ tag["tag_name"] }</td>
                            <td>{ tag["tag_value"] }</td>
                        </tr>
                        ''' for tag in tags
                    ])
                }
            </tbody>
        </table>
    """


def sqlmetadatastore_triggers_table(triggers: List[Dict[str, str]], triggers_endpoint: str):
    return f"""
        <table class="table is-bordered is-striped is-hoverable"
            hx-get="{ triggers_endpoint }" hx-trigger="every 1s" hx-swap="outerHTML" hx-target="this">
            <thead>
                <tr>
                    <th>Trigger ID</th>
                    <th>Run Triggered</th>
                    <th>Triggered By</th>
                    <th>Trigger Time</th>
                    <th>Message</th>
                </tr>
            </thead>
            <tbody>
                {
                    newline.join([
                        f'''
                        <tr>
                            <th>{ trigger["id"] }</th>
                            <td>{ trigger["run_triggered"] }</td>
                            <td>{ trigger["node_name"] }</td>
                            <td>{ trigger["trigger_time"] }</td>
                            <td>{ trigger["message"] }</td>
                        </tr>
                        ''' for trigger in triggers
                    ])
                }
            </tbody>
        </table>
    """