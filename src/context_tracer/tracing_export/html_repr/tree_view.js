const NODE_COLLAPSED_CLASS = "tree-node-collapsed";
const BUTTON_COLLAPSE_ALL_CLASS = "button-collapse-all";
const BUTTON_COLLAPSE_NODE_CLASS = "button-collapse-node";

/**
 * Collapse all tree nodes below.
 * Collapses all child detail elements including itself.
 */
function collapse_all_tree_nodes_below(node) {
    // Collapse element itself if it is a details tree node
    if (node.classList.contains("tree-node")) {
        node.classList.add(NODE_COLLAPSED_CLASS);
    }
    // Collapse all tree nodes inside
    const child_nodes = node.querySelectorAll(".tree-node");
    for (const child_node of child_nodes) {
        child_node.classList.add(NODE_COLLAPSED_CLASS);
    }
}

/**
 * Expand all tree nodes below.
 * Expands all child detail elements including itself.
 */
function expand_all_tree_nodes_below(node) {
    // Expand element itself if it is a details tree node
    if (node.classList.contains("tree-node")) {
        node.classList.remove(NODE_COLLAPSED_CLASS);
    }
    // Expand all tree nodes inside
    const child_nodes = node.querySelectorAll(".tree-node");
    for (const child_node of child_nodes) {
        child_node.classList.remove(NODE_COLLAPSED_CLASS);
    }
}


/**
 * Create a collapse all button.
 * Collapses all child detail elements including itself.
 * Expands all child detail elements including itself when clicked again.
 */
function create_collapse_all_button() {
    const button = document.createElement("button");
    button.addEventListener("click", function() {
        const node = button.parentElement.parentElement;
        if (!node.classList.contains("tree-node")) {
            console.warn("`create_collapse_all_button` node is not a tree node!")
        }
        if (node.classList.contains(NODE_COLLAPSED_CLASS)){
            // Expand
            expand_all_tree_nodes_below(node)
        } else {
            // Collapse
            collapse_all_tree_nodes_below(node)
        }
    });
    button.classList.add("button-collapse-all");
    return button;
}


/**
 * Collapse node button.
 * Collapse only the node itself, not its children.
 */
function create_collapse_node_button() {
    const button = document.createElement("button");
    button.classList.add(BUTTON_COLLAPSE_NODE_CLASS);
    button.addEventListener("click", function() {
        const node = button.parentElement.parentElement;
        node.classList.toggle(NODE_COLLAPSED_CLASS);
    });
    return button;
}

/**
 * Render the tree as HTML.
 * Tree is collapsible by using details/summary tags.
 * Tree node data is rendered as a visual representation of a JSON object
 */
function render_tree(data, parent) {
    // Node element as details
    const node = document.createElement("div");
    node.classList.add("tree-node");
    parent.appendChild(node);
    // Node header
    const header = document.createElement("div");
    header.classList.add("tree-node-header");
    // Collapse node button
    if (data.children && data.children.length > 0) {
        header.appendChild(create_collapse_node_button());
    }
    // Name in bold
    const name = document.createElement("span");
    name.style.fontWeight = "bold";
    name.innerHTML = data.name
    header.appendChild(name);
    // Collapse all button when node has any
    if (data.children && data.children.length > 0) {
        header.appendChild(create_collapse_all_button());
    }
    // Collapse all button
    node.appendChild(header);
    const tree_node_body = document.createElement("div");
    tree_node_body.classList.add("tree-node-body");
    node.appendChild(tree_node_body);
    // Render data
    if (data.data) {
        // Render data as json
        const data_json = jsonview.create(data.data);
        jsonview.render(data_json, tree_node_body);
        jsonview.collapse(data_json);
    }
    // Recursively render children
    if (data.children) {
        for (const child of data.children) {
            render_tree(child, tree_node_body);
        }
    }
}
