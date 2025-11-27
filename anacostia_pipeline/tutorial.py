from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse



tutorial_app = FastAPI()

@tutorial_app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return f"""
        <!DOCTYPE html>
        <html>
            <head hx-head="merge">
                <meta hx-preserve="true" charset="UTF-8">
                <meta hx-preserve="true" name="viewport" content="width=device-width, initial-scale=1">
                <title hx-preserve="true">Anacostia Console</title>
                
                <!-- non-minified Htmx -->
                <script hx-preserve="true" src="/static/js/third_party/htmx.js" type="text/javascript"></script>
            </head>
            <body hx-ext="head-support">
                <div id="page_content">
                    <h1>Anacostia Pipeline Tutorial</h1>
                    <p>Welcome to the Anacostia Pipeline tutorial server!</p>
                </div>
            </body>
        </html>
    """