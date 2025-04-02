function filterList(list, searchBox, filterFn=x=>x.innerHTML.toUpperCase().includes(searchBox.value.toUpperCase())) {
    console.log("You searched " + searchBox.value);
    for(let el of list) {
        if(filterFn(el)) {
            el.style.display="";
        }
        else {
            el.style.display="none";
        }
    }
}