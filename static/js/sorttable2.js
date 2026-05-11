defaults = {};
directions = {};

arrowCodes = ['21E9', '21F3', '21E7'];

function getRowComparator(colNum, direction=1) {
    function compare(row1, row2) {
        let cell1 = row1.children[colNum];
        let cell2 = row2.children[colNum];
        let cv1 = cell1.hasAttribute('sortval') ? cell1.getAttribute('sortval') : cell1.innerHTML;
        let cv2 = cell2.hasAttribute('sortval') ? cell2.getAttribute('sortval') : cell2.innerHTML;
        if(cv1 < cv2) {
            return -direction;
        }
        else if(cv1 > cv2) {
            return direction;
        }
        return 0;
    }
    return compare;
}

function sortTable(id, colNum) {
    let sortCol = colNum+1;
    if(id) {
        let tbody = document.querySelector(`#${id} > tbody`);
        let rows = Array.prototype.slice.call(tbody.children, 1);
        if(!(id in defaults)) {
            defaults[id] = rows;
        }
        if(id in directions && Math.abs(directions[id]) === sortCol) {
            //already sorted increasing by this column; sort decreasing now
            if(sortCol === directions[id]) {
                directions[id] = -sortCol;
                let sortFn = getRowComparator(colNum, -1);
                let sortedRows = rows.toSorted(sortFn);
                for(let row of sortedRows) {
                    //From https://developer.mozilla.org/en-US/docs/Web/API/Node/appendChild
                    //If the given child is a reference to an existing node in the document,
                    //appendChild() moves it from its current position to the new position.
                    tbody.appendChild(row);
                }
            }
            //already sorted decreasing by this column; revert to default now
            else {
                directions[id] = 0;
                let sortedRows = defaults[id];
                for(let row of sortedRows) {
                    tbody.appendChild(row);
                }
            }
        }
        else {
            //default sort; sort increasing now
            directions[id] = sortCol;
            let sortFn = getRowComparator(colNum);
            let sortedRows = rows.toSorted(sortFn);
            for(let row of sortedRows) {
                tbody.appendChild(row);
            }
        }
        let direction = (0 === directions[id]) ? 0 : (directions[id]/sortCol);
        let arrow = `&#x${arrowCodes[direction+1]};`;
        let btn = tbody.children[0].querySelectorAll('th')[colNum]?.querySelector('button');
        if(btn) {
            btn.innerHTML = arrow;
        }
    }
    else {
        console.log("Can't sort a table that doesn't have an ID");
    }
}