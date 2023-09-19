document.addEventListener("DOMContentLoaded", function() {
    const svg = d3.select("body").append("svg")
        .attr("width", 500)
        .attr("height", 500);

    // Example: Drawing a circle using D3.js
    svg.append("circle")
        .attr("cx", 250)
        .attr("cy", 250)
        .attr("r", 50)
        .attr("fill", "blue");
});
