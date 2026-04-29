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
            inputField.value = Array.prototype.map.call(div.getElementsByClassName("infoDiv"), getValueFrom).join(",");//Array.prototype.flatMap.call(div.getElementsByClassName("infoDiv"), x => Array.prototype.filter.call(x.childNodes, y => y.nodeType==Node.TEXT_NODE)).map(n=>n.textContent).join(",");
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
            let existing = new Set(Array.from(divContainer.getElementsByTagName("div")).flatMap(e => Array.from(e.childNodes).filter(n => n.nodeType == Node.TEXT_NODE)).map(n => n.textContent));
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

function redirectToLogin(nextUrl) {
    setTimeout(function() {
        sessionStorage.nextUrl = nextUrl;
        window.location.replace("/login");
    }, 5000);
}