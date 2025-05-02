function getKeywordSuggestions() {
    let keywordBox = document.getElementById("user_keywords");
    let wordsSoFar = keywordBox.value;
    let curPos = keywordBox.selectionStart;
    let wordsList = wordsSoFar.split(/, */);
    if(wordsList.filter(w => w.length>0).length > 0) {
        let whichWord = (wordsSoFar.substring(0, curPos).match(/, */g) || []).length;
        let theWord = wordsList[whichWord];
        if(theWord.length === 0) {
            refreshKeywordSuggestions([]);
        }
        else if(keywordsList?.length > 0) {
            let filteredKeywords = keywordsList.filter(kw => kw.toUpperCase().includes(theWord.toUpperCase()))
            refreshKeywordSuggestions(filteredKeywords.map(x => x.trim()).toSorted());
        }
    }
    else {
        refreshKeywordSuggestions([]);
    }
}

function refreshKeywordSuggestions(suggestions) {
    let keywordSuggestionBox = document.getElementById("keywordSuggestionBox");
    if(keywordSuggestionBox) {
        keywordSuggestionBox.innerHTML = "";
    }
    else {
        keywordSuggestionBox = document.createElement("div");
        keywordSuggestionBox.id = "keywordSuggestionBox";
        try {
            document.getElementById("user_keywords").parentElement.appendChild(keywordSuggestionBox);
        }
        catch(e) {
            console.log(e);
        }
    }
    if(suggestions?.length>0) {
        keywordSuggestionBox.style.display = "";
    }
    else {
        keywordSuggestionBox.style.display = "none";
    }
    for(let suggestion of suggestions) {
        let btn = document.createElement("div");
        btn.innerHTML = suggestion;
        btn.addEventListener("click", function() {
            let keywordBox = document.getElementById("user_keywords");
            let wordsSoFar = keywordBox.value;
            let curPos = keywordBox.selectionStart;
            let precedingCommas = wordsSoFar.substring(0, curPos).match(/, */g);
            let lastComma = (precedingCommas && precedingCommas.length>0) ? (precedingCommas[precedingCommas.length-1]) : "";
            let wordStart = (precedingCommas && precedingCommas.length > 0) ? (wordsSoFar.lastIndexOf(lastComma, curPos)+lastComma.length) : 0
            let procedingCommas = wordsSoFar.substring(curPos).match(/, */g);
            let firstComma = (procedingCommas && procedingCommas.length > 0) ? (procedingCommas[0]) : "";
            let wordEnd = (procedingCommas && procedingCommas.length > 0) ? wordsSoFar.indexOf(firstComma, curPos) : wordsSoFar.length;
            let chars = Array.from(wordsSoFar);
            let newText = chars.toSpliced(wordStart, wordEnd-wordStart, suggestion).join("");
            keywordBox.value = newText;
            keywordBox.focus();
            keywordBox.selectionStart = wordStart + suggestion.length;
            keywordBox.selectionEnd = keywordBox.selectionStart;
            document.getElementById("keywordSuggestionBox").innerHTML = "";
        });
        keywordSuggestionBox.appendChild(btn);
    }
}