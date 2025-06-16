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
            <div>Server Origin: { node_model['origin_url'] }</div>
            <div>Node Type: { node_model['type'] }</div>
            <!-- Note: add another div to display node type information -->
            <div hx-get="{ node_model['status_endpoint'] }" hx-trigger="load, every 1s" hx-target="this" hx-swap="innerHTML"></div>
        </div>
        <div class="node_header_bar_btn">
            <div class="collapse-btn">▲</div>
        </div>
    </div>
    """



def head_template(user_elements: str = "") -> str:
    """
    The template for the <head> tag of the Anacostia Pipeline landing page

    args:
        user_elements: str - The user's custom elements to be included in the <head> tag
    """

    return f"""
    <head hx-head="merge">
        <meta hx-preserve="true" charset="UTF-8">
        <meta hx-preserve="true" name="viewport" content="width=device-width, initial-scale=1">
        <title hx-preserve="true">Anacostia Console</title>
        
        <!-- non-minified Htmx -->
        <script hx-preserve="true" src="/static/js/third_party/htmx.js" type="text/javascript"></script>

        <!-- htmx server-sent events extension -->
        <script hx-preserve="true" src="/static/js/third_party/sse.js"></script>

        <!-- htmx head-support extension -->
        <script hx-preserve="true" src="/static/js/third_party/head-support.js"></script>
        
        <!-- start of user-defined CSS -->
        {user_elements}
        <!-- end of user-defined CSS -->
        
        <!-- custom CSS for Anacostia landing page -->
        <link hx-head="re-eval" rel="stylesheet" type="text/css" href="/static/css/styles/home.css">
        <link hx-head="re-eval" rel="icon" href="/static/img/favicon.ico" type="image/x-icon">

        <!-- custom CSS for node bar -->
        <link hx-head="re-eval" rel="stylesheet" type="text/css" href="/static/css/styles/node_bar.css">
    </head> 
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

    dropdown_content = []

    # for metadata store nodes with /home endpoint, add a link to the dropdown tab, set the color to #F69C9E (light red)
    dropdown_content.extend([
        f'''<a id="{node["id"]}_tab" style="background-color:#F69C9E;" hx-get="{node["endpoint"]}" hx-target="#page_content" hx-swap="innerHTML" hx-trigger="click">{node["label"]}</a>'''
        for node in nodes if node["endpoint"] != "" and node["base_type"] == "BaseMetadataStoreNode"
    ])

    # for metadata store nodes without /home endpoint, add a link to the dropdown tab, set the color to #d04427 (dark red) and disable the link
    dropdown_content.extend([
        f'''<a id="{node["id"]}_tab" style="background-color:#d04427; cursor: not-allowed;">{node["label"]}</a>'''
        for node in nodes if node["endpoint"] == "" and node["base_type"] == "BaseMetadataStoreNode"
    ])

    # for resource nodes with /home endpoint, add a link to the dropdown tab, set the color to #A8D5B1 (light green)
    dropdown_content.extend([
        f'''<a id="{node["id"]}_tab" style="background-color:#A8D5B1;" hx-get="{node["endpoint"]}" hx-target="#page_content" hx-swap="innerHTML" hx-trigger="click">{node["label"]}</a>'''
        for node in nodes if node["endpoint"] != "" and node["base_type"] == "BaseResourceNode"
    ])

    # for resource nodes without /home endpoint, add a link to the dropdown tab, set the color to #89BE90 (dark green) and disable the link
    dropdown_content.extend([
        f'''<a id="{node["id"]}_tab" style="background-color:#89BE90; cursor: not-allowed;">{node["label"]}</a>'''
        for node in nodes if node["endpoint"] == "" and node["base_type"] == "BaseResourceNode"
    ])

    # for action nodes with /home endpoint, add a link to the dropdown tab, set the color to #CBDDE9 (light blue)
    dropdown_content.extend([
        f'''<a id="{node["id"]}_tab" style="background-color:#CBDDE9;" hx-get="{node["endpoint"]}" hx-target="#page_content" hx-swap="innerHTML" hx-trigger="click">{node["label"]}</a>'''
        for node in nodes if node["endpoint"] != "" and node["base_type"] == "BaseActionNode"
    ])

    # for action nodes without /home endpoint, add a link to the dropdown tab, set the color to #809CC8 (dark blue) and disable the link
    dropdown_content.extend([
        f'''<a id="{node["id"]}_tab" style="background-color:#809CC8; cursor: not-allowed;">{node["label"]}</a>'''
        for node in nodes if node["endpoint"] == "" and node["base_type"] == "BaseActionNode"
    ])

    dropdown_content = newline.join(dropdown_content)

    return f"""
    <!DOCTYPE html>
    <html>
        { head_template() }
        <body hx-ext="head-support">
            <nav class="home-navbar">
                <img src="/static/img/dag-black.svg" alt="Home" hx-get="/dag_page" hx-target="this" hx-swap="none" hx-trigger="click">
                <a href="#" class="home-navbar-title">Anacostia Pipeline</a>
                <div class="dropdown">
                    <button class="dropdown-button">Nodes ▽</button>
                    <div class="dropdown-content">
                        { dropdown_content }
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
            <div id="page_content">
                
                <!-- Load CSS for DAG before <svg> and D3, Dagre, Dagre-D3 to allow page to load quicker -->
                <link rel="stylesheet" type="text/css" href="/static/css/styles/dag.css">

                <div id="graph" hx-ext="sse" sse-connect="{graph_sse_endpoint}" sse-swap="WorkUpdate">
                    <svg><g/></svg> 
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