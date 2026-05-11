const unregistered = {"collaborator": {}, "owner": {}};

window.onload = function() {
    const projectsPromise = fetch("/api/v2.0/projects");
    const datasetsPromise = fetch("/api/v2.0/datasets");
    const collectionsPromise = fetch("/api/v2.0/collections");
    projectsPromise.then(resp => resp.json()).then(function(projectsList) {
        const psList = document.getElementById("projects_list");
        for(const p of projectsList) {
            let opt = document.createElement("option");
            opt.value=p.proj_uid;
            opt.innerHTML = `${p.proj_uid}: ${p.title}`;
            psList.appendChild(opt);
        }
    });
    datasetsPromise.then(resp => resp.json()).then(function(datasetsList) {
        const dsList = document.getElementById("datasets_list");
        for(const d of datasetsList) {
            let opt = document.createElement("option");
            opt.value = d.dataset_uid;
            opt.innerHTML = `${d.dataset_uid}: ${d.title}`;
            dsList.appendChild(opt);
        }
    });
    collectionsPromise.then(resp => resp.json()).then(function(collectionsList) {
        if(collectionsList.length > 0) {
            const csIn = document.getElementById("parent_collections_input")
            if(csIn) {
                csIn.style.display="";
                csIn.previousElementSibling.style.display="";
            }
            const csList = document.getElementById("collections_list");
            if(csList) {
                for(const c of collectionsList) {
                    let opt = document.createElement("option");
                    opt.value = c.collection_id;
                    opt.innerHTML = `${c.collection_id}: ${c.title}`;
                    csList.appendChild(opt);
                }
            }
        }
    });
}
var isFixing = false;
function fixValues(formElement) {
    let divs = formElement.getElementsByClassName("multiSelectDiv");
    isFixing = true;
    for(let div of divs) {
        let inputFieldId = Array.from(div.id.matchAll(/(?:^|[A-Z])[a-z]*/g)).map(x => x[0].toLowerCase()).map(x => "div"===x ? "input" : x).join("_");
        let inputField = document.getElementById(inputFieldId);
        if(inputField) {
            function getValueFrom(infoDiv) {
                if(infoDiv.hasAttribute("realvalue")) {
                    return infoDiv.getAttribute("realvalue");
                }
                else {
                    return Array.prototype.filter.call(infoDiv.childNodes, cn => cn.nodeType === Node.TEXT_NODE).map(node => node.textContent).join(",");
                }
            }
            let values = Array.prototype.map.call(div.getElementsByClassName("infoDiv"), getValueFrom);
            inputField.value = values.join(",");
            let ucField = document.getElementById("unregisteredCollaborators");
            if(ucField) {
                ucField.value = JSON.stringify(unregistered["collaborator"]);
            }
            let uoField = document.getElementById("unregisteredOwners");
            if(uoField) {
                uoField.value = JSON.stringify(unregistered["owner"]);
            }
        }
    }
    let editables = formElement.getElementsByClassName("editable");
    let index = 0;
    for(let e of editables) {
        let value = e.innerHTML;
        let name = e.hasAttribute("name") ? e.getAttribute("name") : "field${index}";
        let ip = document.createElement("textarea");
        ip.hidden = true;
        ip.name = name;
        ip.innerHTML = value;
        formElement.appendChild(ip);
        index++;
    }
    isFixing = false;
}
function makeInfoOf(textInfo, realVal) {
    let e = document.createElement("div");
    e.classList.add("infoDiv");
    e.innerHTML = textInfo;
    let btn = document.createElement("button");
    btn.innerHTML = "×";
    btn.type = "button";
    btn.addEventListener("click", function() {
        let par = e.parentElement;
        if(par) {
            par.removeChild(e);
        }
    });
    if(realVal) {
        e.setAttribute("realvalue", realVal);
        e.title = realVal;
    }
    e.appendChild(btn);
    return e;
}
function onInputChange(inputBox) {
    if(inputBox.id && !isFixing) {
        let tokens = inputBox.id.split("_");
        let camelCase = tokens[0] + tokens.slice(1).map(x => x.length>0? (x[0].toUpperCase()+x.slice(1)) : "").join("");
        let divContainer = document.getElementById(camelCase.replace("Input", "Div"));
        if(divContainer) {
            let existing = new Set(Array.from(divContainer.getElementsByTagName("div")).flatMap(e => Array.from(e.childNodes).filter(n => n.nodeType == Node.TEXT_NODE)).map(n => n.textContent));
            tokens = Array.from(inputBox.value.matchAll(/[pc]?[0-9]{6,}/g));
            for(let token of tokens) {
                if(!existing.has(token[0])) {
                    divContainer.appendChild(makeInfoOf(token[0]));
                }
                inputBox.value = inputBox.value.replace(token[0], "");
            }
            tokens = inputBox.value.split(",");
            let newValueArr = [];
            for(let token of tokens) {
                if(!!(token.trim())) {
                    newValueArr.push(token);
                }
            }
            inputBox.value = newValueArr.join(",");
        }
    }
}

//Only used for owners and collaborators
function onInputChange2(inputBox) {
    if(inputBox.id && !isFixing) {
        let tokens = inputBox.id.split("_");
        let camelCase = tokens[0] + tokens.slice(1).map(x => x.length>0? (x[0].toUpperCase()+x.slice(1)) : "").join("");
        let divContainer = document.getElementById(camelCase.replace("Input", "Div"));
        if(divContainer) {
            let existing = new Set(Array.from(divContainer.getElementsByTagName("div")).flatMap(e => Array.from(e.childNodes).filter(n => n.nodeType == Node.TEXT_NODE)).map(n => n.title || n.textContent));
            tokens = Array.from(inputBox.value.matchAll(/[A-Za-z]+(([ -])[A-Za-z]+)*,( [A-Za-z\-]+)(([ -])[A-Za-z]+(\.?))*/g)).map(x => x[0]);
            for(let token of tokens) {
                let option = document.getElementById("opt_"+token.trim());
                if(option) {
                    let val = option.value;
                    let title = null;
                    if(option.hasAttribute("realvalue")) {
                        val = option.getAttribute("realvalue");
                        title = option.value;
                    }
                    if(!existing.has(val)) {
                        divContainer.appendChild(title ? makeInfoOf(title, val) : makeInfoOf(val));
                    }
                    inputBox.value = inputBox.value.replace(title ? title : val, "")
                }
                else {
                    let link = document.getElementById(`${inputBox.id.split("_")[1].slice(0,-1)}_${token}`);
                    let orcid = link.href.split("/").slice(-1)[0];
                    if(!existing.has(orcid) && !existing.has(token)) {
                        divContainer.appendChild(makeInfoOf(token, orcid));
                    }
                }
                tokens = inputBox.value.split(";");
                let newValueArr = [];
                for(let token of tokens) {
                    if(!!(token.trim())) {
                        newValueArr.push(token);
                    }
                }
                inputBox.value = newValueArr.join(";");
            }
        }
    }
}

function addPerson(role) {
    let dialog = document.createElement("dialog");
    let label = document.createElement("p");
    label.classList.add("header");
    label.classList.add("small");
    label.innerHTML = "Add a " + (role === "owner" ? "co-owner" : role);
    dialog.appendChild(label);
    let form = document.createElement("form");
    let orcid_input = document.createElement("input");
    orcid_input.placeholder = "Input the user's ORCID";
    let orcid_label = document.createElement("label");
    orcid_label.innerHTML = "ORCID:&nbsp;";
    orcid_input.name = "orcid";
    orcid_input.id="orcid_input";
    orcid_label.setAttribute("for", orcid_input.id);
    form.appendChild(orcid_label);
    form.appendChild(orcid_input);
    form.appendChild(document.createElement("br"));

    let nameLabel = document.createElement("label");
    nameLabel.setAttribute("for", "lastName");
    nameLabel.innerHTML = "Name:&nbsp";
    let lnInput = document.createElement("input");
    lnInput.id = "lastName";
    lnInput.name = "lastName";
    lnInput.placeholder = "Surname";
    form.appendChild(nameLabel);
    form.appendChild(lnInput);
    let fnInput = document.createElement("input");
    fnInput.id = "firstName";
    fnInput.name = "firstName";
    fnInput.placeholder = "Given Name";
    form.appendChild(document.createTextNode(", "));
    form.appendChild(fnInput);
    form.appendChild(document.createTextNode(" "));
    let miInput = document.createElement("input");
    miInput.id = "middleInitial";
    miInput.name = "middleInitial";
    miInput.placeholder = "MI";
    miInput.maxLength = "1";
    miInput.size = "3";
    form.appendChild(miInput);
    form.appendChild(document.createTextNode(". "));
    let suffixInput = document.createElement("input");
    suffixInput.name = "suffix";
    suffixInput.id="suffix";
    suffixInput.placeholder = "Suffix";
    suffixInput.maxLength = 7;
    suffixInput.size = 6;
    form.appendChild(suffixInput);

    orcid_input.addEventListener("input", function(event) {
        let orcid = orcid_input.value;
        let sections = orcid.split("-");
        if(sections.length == 4 && sections[0].length == 4 && sections[1].length == 4 && sections[2].length == 4 && sections[3].length == 4) {
            let people = document.getElementById("people_list").children;
            let matches = Array.prototype.filter.call(people, x => x.getAttribute("realvalue") === orcid);
            if(matches.length > 0) {
                let match = matches[0].value;
                let names = match.split(",").map(part => part.trim());
                lnInput.value = names[0];
                let given = names[1];
                let givens = given.split(" ");
                let suffix = (givens.slice(-1)[0].match(/^([JS]r\.|[IVXLCDM]+)$/g));
                if(suffix) {
                    suffixInput.value = suffix;
                    givens = givens.slice(0, -1);
                }
                if(givens.length > 1) {
                    miInput.value = givens.slice(-1)[0][0];
                    givens = givens.slice(0, -1);
                }
                fnInput.value = givens.join(" ");
            }
        }

    });

    form.appendChild(document.createElement("br"));

    let btn = document.createElement("button");
    btn.classList.add("download");
    btn.innerHTML = "Add " + role;
    form.addEventListener("submit", function(event) {
        event.preventDefault();
        let name = `${lnInput.value}, ${fnInput.value} ${miInput.value}. ${suffixInput.value}`.replaceAll(/\s+/g, " ").replace(" . ", " ").trim();
        let orcid = orcid_input.value;
        let div = document.getElementById(`${role}sDiv`) || document.getElementById(`add${firstLetterToCapital(role)}sDiv`);
        if(div) {
            div.appendChild(makeInfoOf(name, orcid));
        }
        unregistered[role][orcid] = name;
        dialog.close();
    });
    form.appendChild(btn);
    let btn2 = document.createElement("button");
    btn2.classList.add("download");
    btn2.type="button";
    btn2.innerHTML = "Close without adding";
    btn2.addEventListener("click", function(event) {
        dialog.close();
    });
    form.appendChild(btn2);
    dialog.appendChild(form);
    document.body.appendChild(dialog);
    dialog.showModal();
}

function redirectToLogin(nextUrl) {
    setTimeout(function() {
        sessionStorage.nextUrl = nextUrl;
        window.location.replace("/login");
    }, 5000);
}

function firstLetterToCapital(str) {
    if(!str || str.length==0) return str;
    return str.slice(0, 1).toUpperCase() + str.slice(1);
}