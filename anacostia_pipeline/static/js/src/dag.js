var scriptTag = document.querySelector('script[src="/static/js/src/dag.js"]');

var graph_data = scriptTag.getAttribute('graph-data');
graph_data = graph_data.replace(/'/g, '"');
graph_data = JSON.parse(graph_data);

var nodes = graph_data.nodes;
var edges = graph_data.edges;

var viewport_width = window.innerWidth;
var viewport_height = window.innerHeight;

status_gui_info = {
    // Pre-execution States
    "INITIALIZING": {"color": "#7C3AED", "width": 51},      // Purple - setup/initialization
    
    // Waiting States
    "WAITING_RESOURCE": {"color": "#4B5563", "width": 84},  // Darker gray
    "WAITING_METRICS": {"color": "#4B5563", "width": 78},   // Darker gray
    "QUEUED": {"color": "#475569", "width": 46},            // Darker slate
    "PAUSED": {"color": "#4B5563", "width": 46},            // Darker gray
    
    // Active States
    "PREPARATION": {"color": "#1D4ED8", "width": 60},       // Deep blue
    "EXECUTING": {"color": "#1E40AF", "width": 54},         // Navy blue
    "CLEANUP": {"color": "#4338CA", "width": 46},           // Deep indigo
    
    // Completion States
    "COMPLETE": {"color": "#16A34A", "width": 52},          // Darker green
    "TRIGGERED": {"color": "#16A34A", "width": 52},         // Same as complete (green)
    
    // Skipped State
    "SKIPPED": {"color": "#6B7280", "width": 47},           // Medium gray
    
    // Error States
    "FAILURE": {"color": "#DC2626", "width": 37},           // Vivid red
    "ERROR": {"color": "#B91C1C", "width": 31}              // Darker red
}

// Create a new directed graph 
var g = new dagre.graphlib.Graph({ directed: true, compound: false, multigraph: false });

// Set an object for the graph label
g.setGraph({});

// Add nodes to the graph. 
// Note: the order of when the nodes are declared does not determine the layout of the graph.
nodes.forEach((node) => {
    g.setNode(
        node.id, 
        { 
            id: node.id,
            label: node.label, 
            width: 150, 
            height: 100, 
            endpoint: node.endpoint,
            status_endpoint: node.status_endpoint
        }
    );
});

// Add edges to the graph.
edges.forEach((edge) => {
    g.setEdge(
        edge.source, 
        edge.target, 
        {
            id: edge.id, 
            arrowhead: "vee"
        }
    );
});

var graph_bounds = document.getElementById("graph").getBoundingClientRect();
var svg = d3.select("svg");
svg.attr("width", graph_bounds.width * 0.9);
svg.attr("height", graph_bounds.height);

var inner = svg.select("g");

// Set up zoom support
var zoom = d3.zoom().on("zoom", (event) => {
    inner.attr("transform", event.transform);
});
svg.call(zoom);

// Create the renderer
var render = new dagreD3.render();

// Run the renderer. This is what draws the final graph.
render(inner, g);

// select node containers
const node_container = inner.selectAll(".node");

// apply HTMX attributes to the entire node container
node_container.attr("hx-get", (v) => { return g.node(v).endpoint; })
              .attr("hx-trigger", "click")
              .attr("hx-target", "#page_content")
              .attr("hx-swap", "innerHTML");

// apply SVG attributes to the rect element
const rect = inner.selectAll("rect");
rect.attr("rx", 10);
rect.attr("ry", 10);
rect.attr("fill", "white");
rect.attr("stroke", "black");
rect.attr("stroke-width", "1.5");
rect.attr("cursor", "pointer");
rect.attr("class", "outer-rect");

// drawing the "pill" that provides the background for the status text of the node
let labels = inner.selectAll(".node .label");
labels.attr("transform", "translate(0, -30)");
labels.insert("rect", ":first-child")
      .attr("class", "status-rect")
      .attr("opacity", "1.0")
      .attr("rx", 10)
      .attr("height", "15")
      .attr("id", (v) => { return `${g.node(v).id}-pill` });

let node_text = inner.selectAll(".node .label g");
let status_rect = inner.selectAll(".status-rect");
status_rect.attr("transform", function(d, i) {
    // Get the transform attribute from the corresponding node_text element at the same index
    const transform_str = node_text.nodes()[i].getAttribute("transform");
    const first_part = transform_str.split(',')[0];     // yields a string like 'translate(x'
    return `${first_part},10)`;
});

// append tspan elements for additional status updates
const text = inner.selectAll(".node .label g text");
text.append("tspan")
    .attr("space", "preserve")
    .attr("dy", "14")
    .attr("x", "5")
    .attr("font-size", "11")
    .attr("fill", "white")
    .attr("id", (v) => { return `${g.node(v).id}-status` });

// setting initial color of edge and arrowhead
const arrowhead = inner.selectAll(".edgePath defs marker");
arrowhead.attr("fill", "black");

const edge = inner.selectAll(".edgePath path.path");
edge.attr("stroke-width", "1.5");
edge.attr("stroke", "black");

document.body.addEventListener('htmx:sseOpen', (event) => {
    const sse_element = event.detail.elt;

    if (sse_element.id === "graph") {
        console.log('/graph_sse event source opened');

        const graph = document.getElementById("graph");
        graph.addEventListener('htmx:sseBeforeMessage', (event) => {
            event.preventDefault();     // call preventDefault() to prevent the sse-swap="WorkUpdate" from swapping in the data

            const data = JSON.parse(event.detail.data);

            const status_text = document.getElementById(`${data.id}-status`);
            status_text.innerHTML = data.status.toLowerCase().replace("_", " ");

            const status_pill = document.getElementById(`${data.id}-pill`);
            status_pill.setAttribute("fill", status_gui_info[data.status]["color"]);
            status_pill.setAttribute("width", status_gui_info[data.status]["width"]);
        });
    }
});

var initialScale = 1;

// Center the graph horizontally in the svg container
// Question: what happens if the width of the svg container is smaller than the width of the graph?
svg.call(zoom.transform, d3.zoomIdentity.translate((svg.attr("width") - g.graph().width * initialScale) / 2, 20).scale(initialScale));

// Center the graph vertically in the svg container by shrinking the height of the svg container
// svg.attr('height', g.graph().height * initialScale + 40);