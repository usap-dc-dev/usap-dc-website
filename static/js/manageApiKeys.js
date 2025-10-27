var whichUser = undefined;
function showPopup(name) {
    whichUser = name;
    document.getElementById("popupBody").innerHTML = "";
    fetch(`/curator/manage_api_keys/${name}`).then(resp => resp.text()).then(function(text) {
        //document.getElementById("popupBody").innerHTML = text;
        let container = document.getElementById("popupBody");
        if(container) {
            let entries = JSON.parse(text);
            if(entries.length > 0) {
                let table = document.createElement("table");
                let tbody = document.createElement("tbody");
                table.appendChild(tbody);
                let tr = document.createElement("tr");
                let columns = getAllKeys(entries);
                for(let col of columns) {
                    let th = document.createElement("th");
                    th.innerHTML = toTitleCase(col.replace("_", " "));
                    tr.appendChild(th);
                }
                let th = document.createElement("th");
                th.innerHTML = "Actions";
                tr.appendChild(th);
                tbody.appendChild(tr);
                for(let entry of entries) {
                    tr = document.createElement("tr");
                    for(let col of columns) {
                        let cell = document.createElement("td");
                        let value = entry[col];
                        if(col === "created") {
                            cell.innerHTML = Intl.DateTimeFormat("en-US", {
                                dateStyle: "medium",
                                timeStyle: "long",
                                timeZone: "UTC"
                            }).format(new Date(value));
                        }
                        else if(col === "other_users") {
                            cell.innerHTML = entry[col].join("; ");
                        }
                        else {
                            cell.innerHTML = entry[col].toString();
                            if(col === "alias") {
                                let btn = document.createElement("button");
                                btn.innerHTML = "✎";
                                btn.classList.add("updateAliasBtn");
                                btn.addEventListener("click", function() {
                                    if("✓" === btn.innerHTML) {
                                        let inputBox = cell.getElementsByTagName("input")[0];
                                        let newAlias = inputBox.value;
                                        cell.removeChild(inputBox);
                                        btn.innerHTML = "✎";
                                        fetch(`/curator/manage_api_keys/${name}/${entry.created}/updateAlias?newAlias=${newAlias}`)
                                        .then(resp => resp.text()).then(function(text) {
                                            let newTextNode = document.createTextNode(text);
                                            cell.insertBefore(newTextNode, btn);
                                        });
                                    }
                                    else {
                                        let prevAliasNode = Array.from(cell.childNodes).filter(node => node.nodeType === Node.TEXT_NODE)[0];
                                        if(prevAliasNode) {
                                            cell.removeChild(prevAliasNode);
                                        }
                                        let input = document.createElement("input");
                                        input.value = entry[col].toString();
                                        cell.insertBefore(input, btn);
                                        btn.innerHTML = "✓"
                                    }
                                    
                                });
                                cell.appendChild(btn);
                            }
                        }
                        tr.appendChild(cell);
                    }
                    let actionsCell = document.createElement("td");
                    function makeActionBtns() {
                        actionsCell.innerHTML = "";
                        let actions = ("valid" === entry.status.toLowerCase())?(["Suspend", "Disable"]):(("suspended" === entry.status.toLowerCase())?(["Reinstate", "Disable"]):([]));
                        for(let action of actions) {
                            let btn = document.createElement("button");
                            btn.innerHTML = action;
                            btn.addEventListener("click", function() {
                                fetch(`/curator/manage_api_keys/${name}/${entry.created}/${action}`).then(function(response) {
                                    return response.text();
                                }).then(function(txt) {
                                    let theRow = tbody.querySelectorAll("tr")[entries.indexOf(entry)+1];
                                    entry.status = txt;
                                    theRow.querySelectorAll("td")[columns.indexOf("status")].innerHTML = txt;
                                    makeActionBtns();
                                });
                            })
                            actionsCell.appendChild(btn);
                        }
                    }
                    makeActionBtns();
                    tr.appendChild(actionsCell);
                    tbody.appendChild(tr);
                }
                container.appendChild(table);
            }
            let addKeyBtn = document.createElement("button");
            addKeyBtn.innerHTML = "Add New API Key";
            addKeyBtn.addEventListener("click", function() {
                fetch(`/curator/manage_api_keys/new/${name}`).then(response => response.json()).then(function(obj) {
                    let newKey = obj.key;
                    let username = obj.user;
                    let infoP = document.createElement("p");
                    infoP.innerHTML = `You have created a new API key for ${username}: <code onclick='navigator.clipboard.writeText(\"${newKey}\")'>${newKey}</code> (click to copy). Send this to the user and ensure that they save it.`;
                    container.appendChild(infoP);
                });
            });
            container.appendChild(addKeyBtn);
        }
        document.getElementById("apiKeyPopup").style.display = "block";
    });
}

function reloadPopup() {
    showPopup(whichUser);
}

function toTitleCase(inputStr) {
    return inputStr.split(" ").map(function(word) {
        if(word.length > 0) {
            return word[0].toUpperCase() + word.slice(1);
        }
        return "";
    }).join(" ");
}

function getAllKeys(listOfObjs) {
    let allKeys = new Set();
    for(let obj of listOfObjs) {
        for(let key of Object.keys(obj)) {
            allKeys.add(key);
        }
    }
    return Array.from(allKeys);
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