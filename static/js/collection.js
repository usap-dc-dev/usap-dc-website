var editing = false;

function createNewCollection(parentCollectionIds) {
    window.location.href = "/submit/collection?parents=" + parentCollectionIds.join(",");
}

function addProject(collectionId, projectId=null) {
    if(projectId) {

    }
}

function addDataset(collectionId, datasetId=null) {
    if(datasetId) {

    }
}

function toggleEditing() {
    document.getElementById("add_projects_div").hidden = editing;
    document.getElementById("add_datasets_div").hidden = editing;
    document.getElementById("submitBtn").hidden = editing;
    editing = !editing;
    if(editing) {
        document.getElementById("editBtn").innerHTML = document.getElementById("submitBtn").innerHTML;
    }
    else {
        document.getElementById("editBtn").innerHTML = "Edit";
        document.getElementById("submitBtn").click();
    }
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