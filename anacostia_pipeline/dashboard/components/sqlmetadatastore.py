from typing import List, Dict

from .node_bar import node_bar_closed



newline = "\n"

def sqlmetadatastore_runs_table(runs: List[Dict[str, str]], runs_endpoint: str):
    return f"""
        <table class="table is-bordered is-striped is-hoverable"
            hx-get="{ runs_endpoint }" hx-trigger="every 1s" hx-swap="outerHTML" hx-target="this">
            <thead>
                <tr>
                    <th>Run ID</th>
                    <th>Start Time</th>
                    <th>End Time</th>
                </tr>
            </thead>
            <tbody>
                {
                    newline.join([
                        f'''
                        <tr>
                            <th>{ run["id"] }</th>
                            <td>{ run["start_time"] }</td>
                            <td>{ run["end_time"] }</td>
                        </tr>
                        ''' for run in runs
                    ])
                }
            </tbody>
        </table>
    """


def sqlmetadatastore_home(header_bar_endpoint: str, data_options: Dict[str, str], runs: List[Dict[str, str]]):
    return f"""
        {node_bar_closed(header_bar_endpoint)}

        <!-- Note: the /static directory is not mounted here, but in the main webserver -->
        <link rel="stylesheet" type="text/css" href="/static/css/styles/sqlmetadatastore.css">

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
                    <th>Created At</th>
                    <th>End Time</th>
                    <th>Location</th>
                    <th>State</th>
                </tr>
            </thead>
            <tbody>
                {
                    newline.join([
                        f'''
                        <tr>
                            <th>{ sample["id"] }</th>
                            <td>{ sample["run_id"] }</td>
                            <td>{ sample["created_at"] }</td>
                            <td>{ sample["end_time"] }</td>
                            <td>{ sample["location"] }</td>
                            <td>{ sample["state"] }</td>
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
                            <td>{ metric["key"] }</td>
                            <td>{ metric["value"] }</td>
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
                            <td>{ param["key"] }</td>
                            <td>{ param["value"] }</td>
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
                            <td>{ tag["key"] }</td>
                            <td>{ tag["value"] }</td>
                        </tr>
                        ''' for tag in tags
                    ])
                }
            </tbody>
        </table>
    """