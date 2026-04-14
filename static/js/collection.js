var editing = false;

function createNewCollection(parentCollectionIds) {
    window.location.href = "/submit/collection?parents=" + parentCollectionIds.join(",");
}

function removeParent(parentCollectionId) {
    let inputField = document.getElementById("rm_parents_input");
    inputField.value = parentCollectionId;
    inputField.dispatchEvent(new Event('input', { bubbles: false}));
}

function toggleEditing() {
    Array.prototype.forEach.call(document.getElementsByClassName("show_when_editing"), e => e.hidden = editing);
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

function removeFromCollection(tableRow) {
    let entryInfo = tableRow.id.split("_");
    let entryType = entryInfo[0];
    let entryId = entryInfo[1];
    let inputBoxId = `rm_${entryType}s_input`;
    document.getElementById(inputBoxId).value = `${entryId}`;
    document.getElementById(inputBoxId).dispatchEvent(new Event('input', { bubbles: false}));
}