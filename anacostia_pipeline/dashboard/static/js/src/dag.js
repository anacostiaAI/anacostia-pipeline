var scriptTag = document.querySelector('script[src="/static/js/src/dag.js"]');

var graph_data = scriptTag.getAttribute('graph-data');
graph_data = graph_data.replace(/'/g, '"');
graph_data = JSON.parse(graph_data);

var nodes = graph_data.nodes;
var edges = graph_data.edges;

var viewport_width = window.innerWidth;
var viewport_height = window.innerHeight;

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
            arrowhead: "vee",
            event_name: edge.event_name
        }
    );
});

var svg = d3.select("svg");
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
rect.attr("stroke", "#333");
rect.attr("stroke-width", "1.5");
rect.attr("cursor", "pointer");

const text = inner.selectAll(".node .label g text");
text.append("tspan")
    .attr("space", "preserve")
    .attr("dy", "1em")
    .attr("x", "1")
    .attr("hx-get", (v) => { return g.node(v).status_endpoint; })
    .attr("hx-target", "this")
    .attr("hx-trigger", "load, every 1s")
    .attr("hx-swap", "innerHTML");

// setting color of edge and arrowhead
const arrowhead = inner.selectAll(".edgePath defs marker");
arrowhead.attr("fill", "#333");

const edge = inner.selectAll(".edgePath path.path");
edge.attr("stroke-width", "1.5");
edge.attr("stroke", "#333");
edge.attr("_", (e) => { 
    return `
    on ${g.edge(e).event_name} from EventStream
        set @stroke to event.data 
        set @fill to event.data in next <marker/>
    end`;
});

var initialScale = 1;

// Center the graph horizontally in the svg container
// Question: what happens if the width of the svg container is smaller than the width of the graph?
svg.call(zoom.transform, d3.zoomIdentity.translate((svg.attr("width") - g.graph().width * initialScale) / 2, 20).scale(initialScale));

// Center the graph vertically in the svg container by shrinking the height of the svg container
// svg.attr('height', g.graph().height * initialScale + 40);