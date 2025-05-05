function getDefaultFilterFn(searchBox) {
    return function(listItem) {
        let searchableHtml = listItem.childNodes[0].innerHTML.toUpperCase();
        let keywords = searchBox.value.toUpperCase().split(/\s+/);
        let includesKeyword = keywords.reduce((acc, cur) => acc && searchableHtml.includes(cur), true);
        return includesKeyword;
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