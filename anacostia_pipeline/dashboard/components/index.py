from typing import List, Dict



def index_template(nodes: List[Dict[str, str]], json_data: str, node_headers: List[str] = []) -> str:
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
    <html>
        <head>
            <meta charset="UTF-8">
            <title>Anacostia Console</title>
            
            <!-- Bulma CSS -->
            <link rel="stylesheet" href="/static/css/bulma.css">
            
            <!-- custom CSS for Anacostia landing page -->
            <link rel="stylesheet" type="text/css" href="/static/css/home.css">
            <link rel="icon" href="/static/img/favicon.ico" type="image/x-icon">

            <!-- custom CSS for node bar -->
            <link rel="stylesheet" type="text/css" href="/static/css/node_bar.css">
            
            <!-- non-minified Htmx -->
            <script src="/static/js/htmx.js" type="text/javascript"></script>

            <!-- minified _Hyperscript (Whole 9 Yards version) -->
            <script src="/static/js/_hyperscript_w9y.min.js"></script>

            <!-- Dependencies for DAG rendering (D3, Dagre, Dagre-D3) -->
            <script src="/static/js/d3.v6.min.js"></script>
            <script src="/static/js/dagre.min.js"></script>
            <script src="/static/js/dagre-d3.min.js"></script>
            <link rel="stylesheet" type="text/css" href="/static/css/dag.css">

            <!-- node_headers -->
            {newline.join([header for header in node_headers])}
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
                                    f'''<a class="navbar-item" hx-get="{node["endpoint"]}" hx-target="#page_content" hx-swap="innerHTML" hx-trigger="click">{node["label"]}</a>'''
                                    for node in nodes
                                ])
                            }
                        </div>
                    </div>
                </div>
            </nav>
            <div id="page_content">
                <div id="graph">
                    <svg width="960" height="700"><g/></svg> 
                    <section id="footer">
                        <p>Potomac AI Inc.</p>
                    </section>
                </div>
                <script src="/static/js/dag.js" graph-data="{json_data}"></script>
            </div>
        </body>
    </html>
    """