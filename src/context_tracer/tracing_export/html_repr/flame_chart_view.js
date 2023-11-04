/**
 * Render a single node of the tree.
 */
function render_node(node_obj, parent) {
    // Node element as details
    const node = document.createElement("div");
    node.classList.add("tree-node");
    parent.replaceChildren(node);
    // Node header
    const header = document.createElement("div");
    header.classList.add("tree-node-header");
    // Name in bold
    const name = document.createElement("span");
    name.style.fontWeight = "bold";
    name.innerHTML = node_obj.name
    header.appendChild(name);
    node.appendChild(header);
    const tree_node_body = document.createElement("div");
    tree_node_body.classList.add("tree-node-body");
    node.appendChild(tree_node_body);
    // Render data
    if (node_obj.data) {
        // Render data as json
        const data_json = jsonview.create(node_obj.data);
        jsonview.render(data_json, tree_node_body);
        jsonview.expand(data_json);
    }
}


/**
 * Render a clickable flame chart.
 *
 * Requires https://github.com/spiermar/d3-flame-graph
 */
function render_flame_chart(
    data_obj,
    parent_container_id,
    selected_node_container_id
) {

    const parent_elem = document.getElementById(parent_container_id)
    // Define flamegraph
    const flame_graph = flamegraph()
        .width(parent_elem.clientWidth)
        .inverted(true)
        .sort(false)
        .transitionDuration(500)
        .setColorHue("aqua")
        .title("");
    // Tooltip hovering
    const tip = flamegraph.tooltip.defaultFlamegraphTooltip()
        .text(d => d.data.name);
    flame_graph.tooltip(tip);
    // Render flamegraph
    d3.select("#"+parent_container_id)
        .datum(data_obj)
        .call(flame_graph);
    // On click
    flame_graph.onClick(function (d) {
        console.info("You clicked on frame "+ d.data.name);
        render_node(d.data, document.getElementById(selected_node_container_id));
    });
    // On resize
    window.onresize = function(event) {
        const element = document.getElementById(parent_container_id);
        flame_graph.width(element.clientWidth);
        flame_graph.update();
    };
    return flame_graph
};
