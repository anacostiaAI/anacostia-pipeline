from typing import List, Dict



def node_bar_invisible(node_model: Dict[str, str]) -> str:
    return f"""
    <div id="{node_model["id"]}_header_div" 
        hx-get="{node_model["header_bar_endpoint"]}" 
        hx-trigger="click from:#{node_model["id"]}, click from:#{node_model["id"]}_tab" 
        hx-swap-oob="true">
    </div>
    """

def node_bar_closed(node_model: Dict[str, str], open_div_endpoint: str) -> str:
    return f"""
    <div id="{node_model["id"]}_header_div" 
        hx-get="{open_div_endpoint}" 
        hx-trigger="click from:#{node_model["id"]}, click from:#{node_model["id"]}_tab, click from:.node_header_bar_btn" 
        hx-swap-oob="true">
        <div class="node_header_bar_btn">
            <div class="collapse-btn">▼</div>
        </div>
    </div>
    """

def node_bar_open(node_model: Dict[str, str], close_div_endpoint: str) -> str:
    return f"""
    <div id="{node_model["id"]}_header_div" 
        hx-get="{close_div_endpoint}" 
        hx-trigger="click from:#{node_model["id"]}, click from:#{node_model["id"]}_tab, click from:.node_header_bar_btn" 
        hx-swap-oob="true">
        <div id="node_header_bar">
            <div>Node name: { node_model['id'] }</div>
            <!-- Note: add another div to display node type information -->
            <div hx-get="{ node_model['status_endpoint'] }" hx-trigger="load, every 1s" hx-target="this" hx-swap="innerHTML"></div>
            <div hx-get="{ node_model['work_endpoint'] }" hx-trigger="load, every 500ms" hx-target="this" hx-swap="innerHTML"></div>
        </div>
        <div class="node_header_bar_btn">
            <div class="collapse-btn">▲</div>
        </div>
    </div>
    """



def index_template(nodes: List[Dict[str, str]], json_data: str, graph_sse_endpoint: str) -> str:
    """
    The template for the Anacostia Pipeline landing page

    args:
        nodes: List[Dict[str, str]] - List of nodes to be displayed in the navbar dropdown
        json_data: str - JSON string representing the DAG
        node_headers: List[Component] - List of node headers to be included in the HTML <head> tag
    
    returns:
        str - HTML template for the Anacostia Pipeline landing page
    """

    newline = "\n"

    return f"""
    <!DOCTYPE html>
    <html>
        <head>
            <meta charset="UTF-8">
            <title>Anacostia Console</title>
            
            <!-- Bulma CSS -->
            <link rel="stylesheet" href="/static/css/third_party/bulma.css">
            
            <!-- custom CSS for Anacostia landing page -->
            <link rel="stylesheet" type="text/css" href="/static/css/styles/home.css">
            <link rel="icon" href="/static/img/favicon.ico" type="image/x-icon">

            <!-- custom CSS for node bar -->
            <link rel="stylesheet" type="text/css" href="/static/css/styles/node_bar.css">
            
            <!-- non-minified Htmx -->
            <script src="/static/js/third_party/htmx.js" type="text/javascript"></script>

            <!-- minified _Hyperscript (Whole 9 Yards version) -->
            <script src="/static/js/third_party/_hyperscript_w9y.min.js"></script>

            <!-- htmx server-sent events extension -->
            <script src="/static/js/third_party/sse.js"></script>
        </head>
        <body>
            <nav class="navbar" role="navigation" aria-label="main navigation">
                <div class="navbar-brand">
                    <a class="navbar-item dag-icon" href="/"><img src="/static/img/dag-black.svg" alt="Home"></a>
                    <h1 class="navbar-item title">Anacostia Pipeline</h1>
                </div>
                <div class="navbar-end">
                    <div class="navbar-item has-dropdown is-hoverable">
                        <a class="navbar-link">Nodes</a>
                        <div class="navbar-dropdown is-right">
                            {newline.join(
                                [
                                    f'''<a id="{node["id"]}_tab" class="navbar-item" hx-get="{node["endpoint"]}" hx-target="#page_content" hx-swap="innerHTML" hx-trigger="click">{node["label"]}</a>'''
                                    for node in nodes
                                ])
                            }
                        </div>
                    </div>
                </div>
            </nav>
            <div>
                { 
                    newline.join(
                        [ node_bar_invisible(node) for node in nodes ]
                    ) 
                }
            </div>
            <div id="page_content" 
                _=" eventsource EventStream from {graph_sse_endpoint}
                        on open 
                            log 'event source {graph_sse_endpoint} connected'
                        end
                    end
                    on htmx:beforeSwap
                        if event.detail.target == htmx.find('#page_content')
                            log 'event source {graph_sse_endpoint} closed'
                            EventStream.close() 
                        end
                    end">
                
                <!-- Load CSS for DAG before <svg> and D3, Dagre, Dagre-D3 to allow page to load quicker -->
                <link rel="stylesheet" type="text/css" href="/static/css/styles/dag.css">

                <div id="graph">
                    <svg width="960" height="700"><g/></svg> 
                    <section id="footer">
                        <p>Potomac AI Inc.</p>
                    </section>
                </div>

                <!-- Dependencies for DAG rendering (D3, Dagre, Dagre-D3) -->
                <script src="/static/js/third_party/d3.v6.min.js"></script>
                <script src="/static/js/third_party/dagre.min.js"></script>
                <script src="/static/js/third_party/dagre-d3.min.js"></script>

                <!-- Custom JS for rendering DAG -->
                <script src="/static/js/src/dag.js" graph-data="{json_data}"></script>
            </div>
        </body>
    </html>
    """