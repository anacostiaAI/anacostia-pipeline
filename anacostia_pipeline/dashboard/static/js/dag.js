var scriptTag = document.querySelector('script[src="/static/js/dag.js"]');

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
            source_name: edge.source,
            target_name: edge.target,
            arrowhead: "vee",
            endpoint: edge.endpoint
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

/*
const arrowhead = inner.selectAll(".edgePath defs marker");
arrowhead.attr("fill", "#333");

const edge = inner.selectAll(".edgePath path.path");
edge.attr("stroke-width", "1.5");
edge.attr("stroke", "#333");
edge.attr("_", (e) => { 
    return `
    on load
        -- create global variables to store the edge's line and arrowhead
        set global ${g.edge(e).source_name}_${g.edge(e).target_name}_path to me
        set global ${g.edge(e).source_name}_${g.edge(e).target_name}_arrowhead to the next <marker/> 

        -- listen for a 'done' event, change the color of the line and arrowhead to the color specified in the data packet
        eventsource event_stream from ${g.edge(e).endpoint} 
            on done as string
                set ${g.edge(e).source_name}_${g.edge(e).target_name}_path @stroke to it 
                set ${g.edge(e).source_name}_${g.edge(e).target_name}_arrowhead @fill to it
            end
            on close as string
                log it
                call event_stream.close()
            end
        end
    end`; 
});

const edge = inner.selectAll(".edgePath path.path");
edge.attr("stroke", "#333");
edge.attr("stroke-width", "1.5");
edge.attr("id", (e) => { 
    return `${g.edge(e).source_name}-${g.edge(e).target_name}`; 
});
edge.attr("_", (e) => { 
    return `
    on load 
        set global ${g.edge(e).source_name}_${g.edge(e).target_name}_path to me
        set global ${g.edge(e).source_name}_${g.edge(e).target_name}_arrowhead to the next <marker/>
    repeat forever 
        fetch ${g.edge(e).endpoint} 
        set ${g.edge(e).source_name}_${g.edge(e).target_name}_path @stroke to result 
        set ${g.edge(e).source_name}_${g.edge(e).target_name}_arrowhead @fill to result
        wait 500ms
    end`; 
});
*/

const edge = inner.selectAll(".edgePath path.path");
edge.attr("stroke", "#333");
edge.attr("stroke-width", "1.5");

/*
edges.forEach(
    (edge) => {
        const eventSource = new EventSource(edge.endpoint);
        
        eventSource.addEventListener('alive', function(event) {
            console.log('SSE message:', event.data);
        });

        eventSource.onopen = function() {
            console.log(`${edge.source} -> ${edge.target} SSE connection opened`);
        };

        eventSource.onerror = function(error) {
            console.error('SSE error:', error);
            eventSource.close();
        };
    }
)
*/

nodes.forEach(
    (node) => {
        const eventSource = new EventSource(`/node/${node.id}/events`);

        eventSource.addEventListener('alive', function(event) {
            //console.log('SSE message:', event.data);
            edge.attr("stroke", (e) => {
                if (g.edge(e).source_name == node.id) {
                    return event.data;
                }
                return "#333"
            });
        });

        eventSource.onopen = function() {
            console.log(`${node.id} SSE connection opened`);
        };

        eventSource.onerror = function(error) {
            console.error('SSE error:', error);
            eventSource.close();
        };
    }
);

/*
const arrowhead = inner.selectAll(".edgePath defs marker");
arrowhead.attr("fill", "#333");
arrowhead.attr("_", (e) => { 
    return `
    on load repeat forever 
        fetch ${g.edge(e).endpoint} 
        set @fill to result 
        wait 500ms
    end`; 
});
*/

var initialScale = 1;

// Center the graph horizontally in the svg container
// Question: what happens if the width of the svg container is smaller than the width of the graph?
svg.call(zoom.transform, d3.zoomIdentity.translate((svg.attr("width") - g.graph().width * initialScale) / 2, 20).scale(initialScale));

// Center the graph vertically in the svg container by shrinking the height of the svg container
// svg.attr('height', g.graph().height * initialScale + 40);