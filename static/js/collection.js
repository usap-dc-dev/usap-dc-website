function createNewCollection(parentCollectionIds) {
    window.location.href = "/submit/collection?parents=" + parentCollectionIds.join(",");
}

function addProject(collectionId, projectId=null) {

}

function addDataset(collectionId, datasetId=null) {

}

function toggleAncestors(button) {
    if(button.title.includes("Show")) {
        button.title = button.title.replace("Show", "Hide");
        button.innerHTML = "–";
    }
    else {
        button.title = button.title.replace("Hide", "Show");
        button.innerHTML = "+";
    }
    for(const div of Array.prototype.filter.call(button.parentElement.children, x => x.tagName.toLowerCase() === "div")) {
        div.hidden = !div.hidden;
    }
}