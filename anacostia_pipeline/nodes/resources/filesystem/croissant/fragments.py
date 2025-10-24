from typing import List, Dict
from anacostia_pipeline.pipelines.fragments import head_template
import json
from typing import List, Dict


newline = "\n"


def data_entry_card(data_card_entry: Dict[str, str], modal_open_endpoint: str = None):
    return f"""
    <div class="model-entry-card">
        <h1>{ data_card_entry["location"] }</h1>
        <div class="model-entry-row">
        </div>

        {
            f'''
            <button class="open-model-card-modal-btn" 
                hx-get="{ modal_open_endpoint }" hx-trigger="click" hx-target="#modal-container" hx-swap="innerHTML">
                Show Model Card
            </button>
            '''
            if modal_open_endpoint is not None else "" 
        }
    </div>
    """

def dataset_registry_home(update_endpoint: str, dataset_entries: List[str] = None):
    return f"""
        {head_template(
            '''
            <!-- CSS for model registry home page -->
            <link rel="stylesheet" href="/static/css/styles/croissant.css">

            <!-- CSS for markdown modal -->
            <link rel="stylesheet" href="/static/css/styles/markdown.css">
            '''
        )}
        <div id="modal-container"></div>
        <div id="model-entries-container">
            <div class="wrap">
                <header class="page">
                    <h1>Dataset Cards</h1>
                </header>

                <section class="grid" aria-label="Datasets" hx-get="{update_endpoint}" hx-trigger="every 1s" hx-target="this" hx-swap="beforeend">
                    { newline.join(dataset_entries) }
                </section>
            </div>
        </div>
    """

def data_card_modal(modal_close_endpoint: str, modal_html_str: str):
    """
    Renders the markdown file in modal window
    """

    return f"""
    <div class="modal-overlay" hx-get="{modal_close_endpoint}" hx-trigger="click target:.modal-overlay" hx-target="#modal-container">
        <div class="markdown-div page-border">
            <pre>{modal_html_str}</pre>
        </div>
    </div>
    """


def distribution_entry(distribution: dict) -> str:
    entries = []

    for resource in distribution:
        if resource["@type"] == "cr:FileObject":
            entries.append(f'''
                <div class="file" aria-label="FileObject {resource["name"]}">
                    <div class="row"><b>Name:</b> {resource["name"]}</div>
                    <div class="row"><b>Encoding:</b> <span class="code">{resource["encodingFormat"]}</span></div>
                    <div class="row"><b>contentUrl:</b> <span class="code">{resource["contentUrl"]}</span></div>
                    <div class="row"><b>sha256:</b> <span class="code">{resource["sha256"]}</span></div>
                </div>
            ''')
        elif resource["@type"] == "cr:FileSet":
            entries.append(f'''
                <div class="file" aria-label="FileSet {resource["name"]}">
                    <div class="row"><b>FileSet:</b> {resource["name"]}</div>
                    <div class="row"><b>Encoding:</b> <span class="code">{resource["encodingFormat"]}</span></div>
                    <div class="row"><b>Includes:</b> <span class="code">{resource["includes"]}</span></div>
                </div>
            ''')

    return f'''
        <summary>Distribution ({len(entries)})</summary>
        <div class="section">
            {newline.join(entries)}
        </div>
    '''



def record_set_entry(record_set: List[Dict]) -> str:
    record_entries = []
    for record in record_set:

        field_entries = []
        for field in record["field"]:
            field_entries.append(
                f'''
                <div class="row">{field["name"]}</div>
                <div class="file" aria-label="FileSet {record["name"]}">
                    <div class="row">
                        <pre>{json.dumps(field, indent=4)}</pre>
                    </div>
                </div>
                '''
            )

        record_entries.append(f'''
            <div class="file" aria-label="FileSet {record["name"]}">
                <div class="row"><b>name:</b> {record["name"]}</div>
                <div class="row"><b>description:</b> {record["description"]}</div>
            </div>
            <div class="row">Fields:</div>
            {newline.join(field_entries)}
        ''')

    return f'''
        <summary>Record Set</summary>
        <div class="section">
            {newline.join(record_entries)}
        </div>
    '''



def card_entry(data_card_path: str) -> str:
    with open(data_card_path) as file:
        data_card = json.load(file)

        return f'''
        <article class="card" role="article" aria-labelledby="ds-1-title">
            <div class="head">
            <span class="badge" aria-label="Type">Dataset</span>
            <h2 class="title" id="ds-1-title">{data_card["name"]}</h2>
            </div>

            <p class="desc">{data_card["description"]}</p>

            <div class="kv" aria-label="Key attributes">
            <div class="item"><b>Published:</b> <time datetime="{data_card["datePublished"]}">{data_card["datePublished"]}</time></div>
            <div class="item"><b>Conforms to:</b> <a href="{data_card["conformsTo"]}">{data_card["conformsTo"]}</a></div>
            <div class="item"><b>License:</b> <a href="{data_card["license"]}">{data_card["license"]}</a></div>
            <div class="item"><b>URL:</b> <a href="{data_card["url"]}">{data_card["url"]}</a></div>
            </div>

            <details>
                {distribution_entry(data_card["distribution"])}
            </details>

            <details>
                {record_set_entry(data_card["recordSet"])}
            </details>

            <details>
                <summary>View Raw JSON-LD</summary>
                <div class="section">
                    View Raw JSON-LD
                </div>
            </details>

        </article>
        '''