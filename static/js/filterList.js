function getDefaultFilterFn(searchBox) {
    return function(listItem) {
        let searchableHtml = listItem.childNodes[0].innerHTML.toUpperCase();
        return searchableHtml.includes(searchBox.value.toUpperCase());
    }
}

function filterList(list, searchBox, filterFn=getDefaultFilterFn(searchBox)) {
    for(let el of list) {
        if(filterFn(el)) {
            el.style.display="";
        }
        else {
            el.style.display="none";
        }
    }
}