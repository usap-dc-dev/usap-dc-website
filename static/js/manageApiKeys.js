function showPopup(name) {
    document.getElementById("popupBody").innerHTML = "";
    fetch(`/curator/manage_api_keys/${name}`).then(resp => resp.text()).then(function(text) {
        document.getElementById("popupBody").innerHTML = text;
        document.getElementById("apiKeyPopup").style.display = "block";
    });
}

function addEntries(table, ...entries) {
    const body = table.querySelector("tbody");
    if(body) {
        for(const entry of entries) {
            let tr = document.createElement("tr");
            for(const key in entry) {
                let td = document.createElement("td");
                td.innerHTML = entry[key];
                tr.appendChild(td);
            }
            body.appendChild(tr);
        }
    }
}

function hidePopup() {
    document.getElementById("apiKeyPopup").style.display = "";
}