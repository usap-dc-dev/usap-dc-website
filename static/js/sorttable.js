/*
*******************************
Quick overview section:

  Author and URL:
    Mingyi Liu (http://mingyi.workhard.us), website for this script: hosted at 
    http://www.mingyi.org/other/ts_demo.html and 
    http://hotscripts.com/Detailed/57817.html

  How to use this script:
    1. Put this script inside the same directory as the HTML file that uses it (
       of course, you can put the script in any web directory that you like).
    2. Put <script src="sorttable.js"></script> in your html page <head> section.
       Or, if you put the sorttable.js in another directory, put the path of that
       directory into the src, e.g. src="/absoluteURLpath/sorttable.js" or src="
       relativeURLpath/sorttable.js"
    3. Add class="sortable" to the tables you want sorted (if your table html tag
       was previously <table>, now it'd be <table class="sortable">; if it used 
       to be <table class="old_style">, now it'd become
       <table class="old_style sortable">)
    That's it! Now you can already sort any "sortable" table on your page!
    If prefered, you could set the variable sort_only_sortable to false to have
    all the tables on your page sortable by default!
  How to sort:
    Either click on the links in the first table row, or simply click left mouse
    button inside any table cell of an HTML table while pressing both the
    "alt" key and "ctrl" (I write the key sequence as 'ctrl+alt+click').
  Is this script free?
    Yes, absolutely. AFAIK, there is NO similar script on the web that's as good 
    as this one, so if you like it, please take some time to vote for it at 
    http://hotscripts.com/Detailed/57817.html.
  License:
    You can do whatever you want with the script, but please at least keep the above
    header section containing the author name and script URL intact. BTW, also
    observe the tradition of most freewares - no warranty provided and use it
    at your own risk! ;)
*********************************
 For advanced usage or more descriptions for the script and its author, read on!

Authors and feature lists:
 This script is written by Mingyi Liu. A small yet significant part of the table
 sorting code was initially based upon an excellent script by Stuart Langridge.
 But now the features and even the sorting algorithm are already way beyond what 
 any other script offered.

 Mingyi's script's feature list:
 Feature list 1 - The power features that are unique to my script (no other
   script on the web has these features AFAIK):
   1. Sort US, European, mixed style and user-specified dates (and time)! It's 
      not trivial!
   2. Support keyboard shortcuts for quick access to sorting (e.g., press 
      'ctrl+alt'+click anywhere in table to sort)!
   3. Support various levels of style preserving (row-level, cell-level or
      table-level (default)) after sorting. Particularly useful for the table 
      rows that're alternatively highlighted (e.g., odds rows yellow, even rows
      blue).
   4. Support for user-supplied sorting function for any custom data type.
   5. Allows table sorting on page loading, force columns to be not sortable or 
      only sortable in one direction. One could even optionally force all table
      to be sortable by default (without having to specify a 'sortable' class).
   6. Sorted tables can remember their states (so when user leaves the page and 
      comes back, the tables remain sorted the way the user left the page with).
      This is also an optional customizable feature.   
   7. Has a very popular FireFox extension (https://addons.mozilla.org/firefox/2637/)
      made out of this script!
 Feature list 2 - you'd be hard pressed to find ALL the features below in any
      other solutions:
   1. Easy to use, runs very fast, X-browser
   2. Support multiple tables on the same html page.
   3. Works with thead, tbody, th, etc. html tags and allows multi-row header 
      and footer.
   4. Intelligent sorting - Automatically detects data types so that
      sorting is correct. For example, this script would recognize that 0.5 and 
      5E-1 are the same values, '06/22/1996 16:59:02' and 
      '06/22/1996 04:59:02 PM' are the same time.
   5. Allow force sorting of unrecognized currencies.
   6. Allow customization of the style of the table heading clickable links. 
      Allows customization of (warning) messages as well.
   7. Improved reliability over offending data (like non-number in a number
       column).    

Advanced usage:
 1. You can change the variables in the sections marked as "modify as needed".
 2. Currency detection is limited to a few common ones (¥£€$), so if you have,
    say, YMB, just add ts_type="money" to the column heading, e.g., 
    <td ts_type="money"> or <th ts_type="money">. That's it! It'll be sorted
    correctly. In fact, it's always a good idea to specify money columns as
    type "money", even if it's US $, euro, etc.
 3. You could also force data type to be "number", "ip", "date" the same way
    as "money". You could even specify ts_type to be "custom", but then you
    must supply valid javascript code that accesses the values a and b and
    evaluates to negative, positive or 0 (like any comparison function). e.g.:
    implement numeric sorting:
      <td ts_type="custom" ts_sortfn="aa=parseFloat(a);bb=parseFloat(b); if(isNaN(aa)) aa=0; if(isNaN(bb)) bb=0; aa-bb;">My number column header cell</td>
    implement your own IP sorting:
      <th ts_type="custom" ts_sortfn="aa=a.split('.');bb=b.split('.');(aa[0] - bb[0] || aa[1] - bb[1] || aa[2] - bb[2] || aa[3] - bb[3]);">My IP column header cell</th>
    Note that:
    a. You must know what you're doing!
    b. Due to the use of eval, the custom function is always slower than 
       defining function in this script directly (but using custom function 
       keeps code here cleaner during updates).
 4. Add 'sortdir="desc"'' on any table header column to indicate that the
    table should be sorted right after page is loaded. Use 'asc' for ascending
    sorting order. Example: <td sortdir="desc">the default sorting column</td>
 5. Use <table preserve_style="cell"> on any table to indicate that the table 
    redrawing after sorting should preserve cell style (override the global
    preserve_style variable that you can set below). The other valid preserve_style
    is "row", which is very useful if you use alternating colors for table rows.
    Note that row style is slower than default, cell style is even slower due to
    more update needed.
    IE has a "design" (read:bug) that prevents setting innerHTML for rows ("design"
    explained in http://msdn.microsoft.com/workshop/author/tables/buildtables.asp#TOM_Create, 
    bug documented for selects at http://support.microsoft.com/default.aspx?scid=kb;en-us;276228#appliesto).
    Therefore for IE, "row" is actually equivalent with "cell" because it is implemented
    the same way (blame IE for the speed!).         
 6. Use <td ts_type="euro_date"> to indicate date format is DD/MM/YY or DD/MM/YYYY
    (variations like 'DD.MM.YY', 'DD MM,YY', 'DD-MM-YYYY' (or any mix-n-match of
    these 5 characters (. ,-/) are all acceptable)
    Using <td ts_type="other_date" ts_date_format="D/M/Y"> you can also indicate 
    it's european dates. <td ts_type="other_date" ts_date_format="Y/M/D"> to indicate
    date format like YY-MM-DD, YY.MM.DD. If Y is double digit, any number
    smaller than 32 is deemed 20xx, otherwise it's 19xx.  Note that D/M/Y (or any
    user-specified format) could just part of the time string (the date part),
    time conversion will be done correctly. Also note that if you choose custom
    date format, due to the time consumed during date conversion, the sorting
    would be much slower (2-3 times as slow).
    The date/time formats accepted are: 
     1. Anything accepted directly by Javascript's Date.parse() function 
       (format best explained at "http://java.sun.com/j2se/1.3/docs/api/java/util/Date.html#parse(java.lang.String)");
     2. Any combination of a date type specified by you and a time (hour, min, 
        seconds) format accepted by Date.parse(). Check the demo html page (http://www.mingyi.org/other/ts_demo.html) for 
        examples.
  7. Remember that there are a large number of parameters that you could customize
     in the script!
  8. If prefered, you could set the variable sort_only_sortable to false to have
     all the tables on your page sortable by default (without specifying 'sortable'
     class)!
  9. BTW, if you want to have your html page including the script validated against
     W3C Validation (http://validator.w3.org) for XHTML 1.0 Strict, you need to get
     rid of the ts_ for all the attributes (like 'ts_type','ts_sortcell', etc.), based
     on Douglas' suggestion.
  10.To designate 2nd row of table should be the one containing sorting links, add
     ts_linkrow="1" to your <table> tag. (use 2 if 3rd row should contain links,
     and so on)
  11. Add 'ts_nosort' to the header row's <td> tag to indicate this column
     shouldn't be sorted. e.g. <td ts_nosort="y">abc</td> disable sorting link
     on the column. You should also set use_ctrl_alt_click to false to disable
     user using keyboard shortcut to get around the no sort rule on this column.
  12.Add 'ts_forcesort' to the header row's <td> tag to indicate this column
     should be sorted only in one direction. e.g. <td ts_forcesort="asc">abc</td>
     makes sure that the sorting on the column is always in ascending order. Use
     "desc" for descending order.
  13. To allow sorted tables remember their state, customize the cookie_days 
      parameter to be 0 (the default, setting session cookie) or positive days. 
      Preferrably you should also give each table a unique ID. If no ID is set, 
      this script will try to generate a unique one for you. 
*/

var ts_version = "1.30"; // final version (except for potential bug-fixing versions) for this sort-only version of my HTML scripts (TableTools script will be out soon containing sort,filter,copy functionalities)
var ts_browser_agt = navigator.userAgent.toLowerCase();   
var ts_browser_is_ie = ((ts_browser_agt.indexOf("msie") != -1) && (ts_browser_agt.indexOf("opera") == -1));

var ml_tsort = {
  ///////////////////////////////////////////////////
  // configurable constants, modify as needed!
  sort_col_title : "Click to Sort", // the popup text for the sorting link in the header columns
  sort_col_asc_title : "Sorted in ascending order", // the popup text for the sorting link in the header column after the column's sorted in ascending order
  sort_col_desc_title : "Sorted in descending order", // the popup text for the sorting link in the header column after the column's sorted in ascending order
  sort_col_class : "abc", // whichever class you want the heading to be
  sort_col_style : "text-decoration:none; font-weight:bold; color:black", // whichever style you want the link to look like
  sort_col_class_post_sort : "def", // whichever class you want the heading for the column that's just sorted
  sort_col_style_post_sort : "text-decoration:none; font-weight:bold; color:red", // whichever style you want the link to look like when the column for the link was sorted
  sort_col_mouseover : "this.style.color='blue'", // what style the link should use onmouseover?
  use_ctrl_alt_click : true, // allow ctrl-alt-click anywhere in table to activate sorting?
  sort_only_sortable : true, // make all tables sortable by default or just make the tables with "sortable" class sortable? (even if a table does not have class "sortable", one could still use ctrl-alt-click to sort, but the table will not have the clickable table heading for sorting)
  minimum_sort_row : 2, // a table has to have at least this many rows to be sorted
  no_sort_above_click : false, // if true, do not sort any table row above the clicked row (overrides table_head_row and table_nohead_row)
  default_sort_asc : true, // if true then by default, the first sort on the table would be in ascending order
  forcesort_msg : 'This column can only be sorted in one way!', // if you set a message here, user will be warned with this message when they try to resort the ts_forcesort column you had set that could only be sorted in one direction. Set it to '' to NOT display a message.
  nosort_title : 'No sorting on this column!', // if you set a title here, the if user moves their move over the ts_nosort column heading they'd see this message displayed and learn that this column is not to be sorted. Set it to '' to NOT display this message on ts_nosort columns 
  cookie_days : -1, // set the days to be 0 (session cookie) or positive to use cookie to record the state of sorted tables so that when user comes back to this page, tables would remain sorted the way they left the page with. Use -1 if you do not want this feature
  
  ///////////////////////////////////////////////////
  // not important ones, provides compatibility with TableTools FF extension
  default_date_style : 'us', // assume 'us' or 'euro'(pean) style of date format when sorting time column
  table_head_row : 0, // do not sort the first ? number of rows of the table with headers
  table_nohead_row : 1, // do not sort the first ? number of rows of the table without headers
  skiprows : 1, // internally used, don't edit this one!

  //////////////////////////////////////////////////
  // speed related constants, modify as needed!
  table_content_might_change : false, // table content could be changed by other JS on-the-fly? if so, some speed improvements cannot be used.
  preserve_style : "", // (row, cell) preserves style for row or cell e.g., row is useful when the table highlights rows alternatively. cell is much slower while no preservation's the fastest by far!
  tell_me_time_consumption : false, // give stats about time consumed during sorting and table update/redrawing.
  
  //////////////////////////////////////////////////////////
  // anything below this line, modify at your own risk! ;)
  smallest_int : -2147483648000, // date parse is in milliseconds, hence the 000
  cookie_table_idx : 0, // used to generate unique id when cookie's enabled by user but user did not give each table a unique id (the recommended way)
  
  get_doc_name : function()
  {
    var regex = /.+\/(.+)\??/;
    var results = regex.exec(window.location.href);
    if(results) return results[1].replace(/[^A-Za-z0-9]+/g, '');
  },
  
  set_vars : function(event)
  {
    var e = (event)? event : window.event;
    var element = (event)? ((event.target)? event.target : event.srcElement) : window.event.srcElement;
    ml_tsort.clicked_td = ml_tsort.getParent(element,'TD') || ml_tsort.getParent(element,'TH');
    ml_tsort.clicked_table = ml_tsort.getParent(element,'TABLE');

    if(!ml_tsort.clicked_table || ml_tsort.clicked_table.rows.length < 1 || !ml_tsort.clicked_td) return;
    var clicked_cell = ml_tsort.clicked_table.rows[0].cells[ml_tsort.clicked_td.cellIndex];
    if (e.ctrlKey && e.altKey && ml_tsort.use_ctrl_alt_click) ml_tsort.resortTable(clicked_cell);
  },
  
  set_skiprows : function(td)
  {
    ml_tsort.clicked_tr = ml_tsort.getParent(td,'TR');
    ml_tsort.clicked_table = ml_tsort.getParent(td,'TABLE');
    if(ml_tsort.clicked_tr)
    {
      if(ml_tsort.no_sort_above_click)
      {
        ml_tsort.skiprows = 0;
        if(ml_tsort.clicked_table && ml_tsort.clicked_table.rows.length >= ml_tsort.minimum_sort_row)
        {
          for (var j=0; j < ml_tsort.clicked_table.rows.length; j++)
          {
            if(ml_tsort.clicked_table.rows[j] == ml_tsort.clicked_tr) break;
            if(ml_tsort.clicked_table.rows[j].parentNode.tagName.toLowerCase() == 'tbody')
              ml_tsort.skiprows++;
          }
        }
      }
      else
        ml_tsort.skiprows = (ml_tsort.table_has_header(ml_tsort.clicked_table))? ml_tsort.table_head_row : ml_tsort.table_nohead_row;
    }
  },

  makeSortable: function(table) 
  {
    if (table.rows && table.rows.length > 0) 
    {
      var rowidx = table.getAttribute("ts_linkrow") || 0;
      var firstRow = table.rows[rowidx];
    }
    if (!firstRow) return;
    var sortCell;
    // make clickable links on the row
    if(this.cookie_days > -1) 
    {
      if(!table.id)
      {
        this.cookie_table_idx = this.cookie_table_idx + 1;
        table.cookieid = this.get_doc_name() + this.cookie_table_idx;
      }
      else table.cookieid = this.get_doc_name() + table.id;
      var tmp = this.get_cookie(table.cookieid);
      if(tmp)
      {
        var oldVals = tmp.split(':');
        sortCell = firstRow.cells[oldVals[0]];
        sortCell.setAttribute('sortdir', oldVals[1]);
      }
    }
    for (var i=0;i<firstRow.cells.length;i++) 
    {
      var cell = firstRow.cells[i];
      if(cell.getAttribute("ts_nosort"))
      {
        if(this.nosort_title) cell.innerHTML = '<span title="'+ this.nosort_title +'">' + cell.innerHTML + '</span>';
      }
      else
      {
        var txt = cell.innerHTML;
        if(cell.getAttribute("sortdir")) 
        {
          if(sortCell) cell.removeAttribute('sortdir');
          else sortCell = cell;
        }
        cell.innerHTML = '<a style="'+this.sort_col_style+'" onMouseOver="this.oldstyle=this.style.cssText;'+this.sort_col_mouseover+'" onMouseOut="this.style.cssText=this.oldstyle;" class="'+this.sort_col_class+'" href="#" title="'+this.sort_col_title+'" onclick="javascript:ml_tsort.resortTable(this.parentNode);return false">'+txt+'</a>';
      }
    }
    if(sortCell) this.resortTable(sortCell);
  },
  
  sortables_init : function() 
  {
      // Find all tables with class sortable and make them sortable
      if (!document.getElementsByTagName) return;
      var tbls = document.getElementsByTagName("table");
      for (var ti=0;ti<tbls.length;ti++) {
          thisTbl = tbls[ti];
          if(!ml_tsort.sort_only_sortable || thisTbl.className.match(/sortable/i))
            ml_tsort.makeSortable(thisTbl);
      }
  },
  
  getParent : function(el, pTagName) 
  {
  	if (el == null) return null;
  	else if (el.nodeType == 1 && el.tagName.toLowerCase() == pTagName.toLowerCase())	// Gecko bug, supposed to be uppercase
  		return el;
  	else
  		return this.getParent(el.parentNode, pTagName);
  },
  
  getInnerText : function(el) 
  {
  	if (typeof el == "string") return el;
  	if (typeof el == "undefined") { return el };
  	if (el.innerText) return el.innerText;	//Not needed but it is faster
  	var str = "";
  	
  	var cs = el.childNodes;
  	var l = cs.length;
  	for (var i = 0; i < l; i++) {
  		switch (cs[i].nodeType) {
  			case 1: //ELEMENT_NODE
  				str += this.getInnerText(cs[i]);
  				break;
  			case 3:	//TEXT_NODE
  				str += cs[i].nodeValue;
  				break;
  		}
  	}
  	return str;
  },
  
  match_date_format : function(value, format)
  {
    format = format.replace(/\//g, '[-,. \\/]');
    this.set_date_array(format);
    if(!isNaN(this.convert_date(this.getInnerText(value.cells[this.sort_column_index])))) return true;
    return false;
  },
    
  get_sortfn : function(warn_type, user_type)
  {
    var sortfn = this.sort_caseinsensitive;
    var table = this.clicked_table;
    var td = this.clicked_td;
    var column = this.sort_column_index;
    if(!table || table.rows.length < ml_tsort.minimum_sort_row || !td) return sortfn;
    var type = user_type || table.rows[0].cells[column].getAttribute("ts_type");
    this.replace_pattern = '';

    // find first non-empty data cell
    var itm;
    for(var rowcount = 0, i = 0; i < table.rows.length; i++)
    {
      if(rowcount++ < ml_tsort.skiprows) continue; // skip head rows
      var t = table.rows[i].parentNode.tagName.toLowerCase();
      if(t == 'tbody' && table.rows[i].cells.length >= column + 1)
      {
        itm = this.getInnerText(table.rows[i].cells[column]);
        if(itm.match(/\S/)) break;
      }
    }
    if(i == table.rows.length) return sortfn;
    itm = ml_trim(itm);
    // Work out a type for the column if necessary
    if(!type || (!user_type && this.table_content_might_change)) // if table might change and the type's not set by user at this moment, then we can't rely on previously set type
    {
      // let's try really hard to check dates separated by - or / or .
      if (this.default_date_style == 'euro' && this.match_date_format(table.rows[i], 'D/M/Y'))
      { sortfn = this.sort_date; type = 'euro_date' }
      else if (this.match_date_format(table.rows[i], 'M/D/Y'))
      { sortfn = this.sort_date; type = 'date' }
      else if (itm.match(/^[¥£€$]/))
      { sortfn = this.sort_currency; type = 'money' }
      else if (itm.match(/^\d{1,3}(\.\d{1,3}){3}$/))
      { sortfn = this.sort_ip; type = 'ip' }
      else if (itm.match(/^[+-]?\s*[0-9]+(?:\.[0-9]+)?(?:\s*[eE]\s*[+-]?\s*\d+)?$/))
      { sortfn = this.sort_numeric; type = 'number' }
    }
    else if(type == 'string') sortfn = this.sort_caseinsensitive;
    else if(type == 'date')
    {
      if(this.match_date_format(table.rows[i], 'M/D/Y')) sortfn = this.sort_date;
      else if(warn_type) alert('The data is not standard time stamp and cannot be parsed! The column will be sorted alphabetically instead.');
    }
    else if(type == 'euro_date')
    {
      if(this.match_date_format(table.rows[i], 'D/M/Y')) sortfn = this.sort_date;
      else if(warn_type) alert('The data is not standard time stamp and cannot be parsed! The column will be sorted alphabetically instead.');
    }
    else if(type == 'year_month_date')
    {
      if(this.match_date_format(table.rows[i], 'Y/M/D')) sortfn = this.sort_date;
      else if(warn_type) alert('The data is not standard time stamp and cannot be parsed! The column will be sorted alphabetically instead.');
    }
    else if(type == 'year_date_month')
    {
      if(this.match_date_format(table.rows[i], 'Y/D/M')) sortfn = this.sort_date;
      else if(warn_type) alert('The data is not standard time stamp and cannot be parsed! The column will be sorted alphabetically instead.');
    }
    else if(type == 'other_date')
    {
      this.set_date_array(td.getAttribute("ts_date_format").replace(/\//g, '[-. \\/]'));
      sortfn = this.sort_date;
    }
    else if(type == 'number') sortfn = this.sort_numeric;
    else if(type == 'ip') sortfn = this.sort_ip;
    else if(type == 'money') sortfn = this.sort_currency;
//       else if(type == 'custom') sortfn = function(aa,bb) { a = this.getInnerText(aa.cells[this.sort_column_index]); b = this.getInnerText(bb.cells[this.sort_column_index]); eval(td.getAttribute("ts_sortfn")) }; // the coding here is shorter but interestingly it's also slower
    else if(type == 'custom') { this.custom_code = td.getAttribute("ts_sortfn"); sortfn = this.custom_sortfn }
    else if(warn_type) { alert("unsupported sorting type!"); }
    table.rows[0].cells[column].setAttribute("ts_type", type); // store type for the convenience of both program and user (no need to use context menu to set type again)
    return sortfn;
  },

  table_has_header : function(table)
  {
    if(table.tHead && table.tHead.rows.length > 0) return true;
    return false;
  },

  resortTable : function(td) 
  {
    if(td == null) return;
    var column = td.cellIndex;
    var table = this.getParent(td,'TABLE');
    this.sort_column_index = column;
    
    // let's make a decision that ts_ attributes should always be denoted on first row (not linkrow) for user convenience.
    if(table.rows[0].cells[column].getAttribute("ts_nosort")) return;

    this.clicked_table = table;
    if(table == null || table.rows.length < ml_tsort.minimum_sort_row) return;
//     this.set_skiprows(this.clicked_td || td);
    this.set_skiprows(td);
    this.clicked_td = td; // this order is important when user specify no_sort_above_click

    // set the data rows
    var newRows = new Array();
    var headcount = 0;
    for (var i=0,j=0,rowcount=0;j<table.rows.length;j++)
    {
      var t = table.rows[j].parentNode.tagName.toLowerCase();
      if(t == 'tbody')
      {
        if(rowcount++ < ml_tsort.skiprows) continue;
        else if(table.rows[j].cells.length >= column + 1) newRows[i++] = table.rows[j];
      }
      else if(t == 'thead') headcount++;
    }
    if(newRows.length == 0) return;
    var time2 = new Date();
  
    // now let's do a lot just to save a little time, if possible at all. ;)
    var lastSortCell = table.getAttribute("ts_sortcell") || 0;
    lastSortCell--; // the processing is used for IE, which treats no attribute as 0, while FF treats 0 as still true.
    var lastSkipRows = table.getAttribute("ts_skiprows") || -1;
    var lastSortType = table.rows[0].cells[column].getAttribute("ts_type");
    var sortingSameColumnAndRow = (table == this.last_sorted_table && column == lastSortCell
      && (!ml_tsort.no_sort_above_click || ml_tsort.skiprows == lastSkipRows));
    table.setAttribute('ts_skiprows',ml_tsort.skiprows);

    var initSortDir = td.getAttribute("sortdir");
    var lastSortDir;
    if(td.getAttribute("ts_forcesort"))
      lastSortDir = (td.getAttribute("ts_forcesort") == 'desc')? 'asc' : 'desc';
    else
      lastSortDir = (sortingSameColumnAndRow)? table.getAttribute("ts_sortdir") : (
                    (initSortDir == 'desc')? 'asc' : (initSortDir == 'asc')? 'desc' : (
                    (this.default_sort_asc)? 'desc' : 'asc'));
      
    // check if we really need to sort
    if(!td.getAttribute("ts_forcesort") && !this.table_content_might_change && sortingSameColumnAndRow)
      newRows.reverse();
    else if(td.getAttribute("ts_forcesort") && !this.table_content_might_change 
            && sortingSameColumnAndRow && td.getAttribute("ts_forcesort") == table.getAttribute("ts_sortdir")) 
    { if(this.forcesort_msg) alert(this.forcesort_msg); return; }  
    else
    {
      var sortfn = this.get_sortfn(true);  
      table.setAttribute("ts_sortcell", column+1);
      newRows.sort(sortfn);
      if (lastSortDir == 'asc') newRows.reverse();
    }
    // set style of heading
    var rowidx = table.getAttribute("ts_linkrow") || 0;
    if(lastSortCell > -1 && table.rows[rowidx].cells[lastSortCell].firstChild.style)
    {
      table.rows[rowidx].cells[lastSortCell].firstChild.oldstyle = this.sort_col_style;
      table.rows[rowidx].cells[lastSortCell].firstChild.style.cssText = this.sort_col_style;
      table.rows[rowidx].cells[lastSortCell].firstChild.className = this.sort_col_class;
    }
    if(table.rows[rowidx].cells[column].firstChild.style)
    {
      table.rows[rowidx].cells[column].firstChild.oldstyle = this.sort_col_style_post_sort;
      table.rows[rowidx].cells[column].firstChild.style.cssText = this.sort_col_style_post_sort;
      table.rows[rowidx].cells[column].firstChild.className = this.sort_col_class_post_sort;
    }
    if (lastSortDir == 'desc') table.setAttribute('ts_sortdir','asc');
    else table.setAttribute('ts_sortdir','desc');
    // set cookie if needed
    if(this.cookie_days > -1) this.set_cookie(table.cookieid, column + ':' + table.getAttribute('ts_sortdir'));
    // has to use tagName otherwise IE complains
    if(lastSortCell > -1 && table.rows[rowidx].cells[lastSortCell].firstChild.tagName) table.rows[rowidx].cells[lastSortCell].firstChild.title = this.sort_col_title;
    if(table.rows[rowidx].cells[column].firstChild.tagName) table.rows[rowidx].cells[column].firstChild.title = ((lastSortDir == 'desc')? this.sort_col_asc_title : this.sort_col_desc_title);

    this.last_sorted_table = table;
       
    var time3 = new Date();
    
    var ps = table.getAttribute("preserve_style") || this.preserve_style;
    if(ps == 'row' && !ts_browser_is_ie) 
    {
      var tmp = new Array(newRows.length);
      for (var i = 0; i < newRows.length; i++) tmp[i] = newRows[i].innerHTML;
      for (var i = 0; i < newRows.length; i++) table.rows[i+headcount+ml_tsort.skiprows].innerHTML = tmp[i];
    }
    else if(ps == 'cell' || (ps == 'row' && ts_browser_is_ie)) 
    {
      var tmp = new Array(newRows.length);
      for (var i = 0; i < newRows.length; i++)
        for (var j = 0; j < newRows[i].cells.length; j++)
        {
          if(!tmp[i]) tmp[i] = new Array(newRows[i].cells.length);
          tmp[i][j] = newRows[i].cells[j].innerHTML;
        }
      for (var i = 0; i < newRows.length; i++)
        for (var j = 0; j < newRows[i].cells.length; j++)
          table.rows[i+headcount+ml_tsort.skiprows].cells[j].innerHTML = tmp[i][j];
    }
    else
    {
      for (var i=0;i<newRows.length;i++) // We appendChild rows that already exist to the tbody, so it moves them rather than creating new ones
        table.tBodies[0].appendChild(newRows[i]);
    } 
    var time4 = new Date();
    if(this.tell_me_time_consumption)
    {
      alert('it took ' + this.diff_time(time3, time2) + ' seconds to do sorting!');
      alert('it took ' + this.diff_time(time4, time3) + ' seconds to do redrawing!');
    }
  },
  
  diff_time : function(time2, time1) 
  {
    return (time2.getTime() - time1.getTime())/1000;
  },
  
  // Mingyi Note: it seems ridiculous to do so much processing for
  // customizable date conversion, should try to find a zbetter way
  // of doing it.
  set_date_array : function(f) 
  {
    var tmp = [['D', f.indexOf('D')], ['M', f.indexOf('M')], ['Y', f.indexOf('Y')]];
    tmp.sort(function(a,b){ return a[1] - b[1]});
    this.date_order_array = new Array(3);
    for(var i = 0; i < 3; i++) this.date_order_array[tmp[i][0]] = '$' + (i + 2);
    this.replace_pattern = f.replace(/[DMY]([^DMY]+)[DMY]([^DMY]+)[DMY]/, '^(.*?)(\\d+)$1(\\d+)$2(\\d+)(.*)$');
  },
  
  process_year : function(y) 
  {
    var tmp = parseInt(y);
    if(tmp < 32) return '20' + y; 
  	else if(tmp < 100) return '19' + y;
  	else return y;
  },
  
  // convert to MM/DD/YYYY (or M/D/YYYY) format
  convert_date : function(a)
  {
    var re = 'RegExp.$1+RegExp.'+this.date_order_array['M']+'+\'/\'+RegExp.'+this.date_order_array['D']+'+\'/\'+this.process_year(RegExp.'+this.date_order_array['Y']+')+RegExp.$5';
    var code = 'if(a.match(/'+this.replace_pattern+'/)) (' + re + '); else a;';
    return Date.parse(eval(code));
  },
  
  sort_date : function(a,b) 
  {
    var atext = ml_tsort.getInnerText(a.cells[ml_tsort.sort_column_index]);
    var btext = ml_tsort.getInnerText(b.cells[ml_tsort.sort_column_index]);
    var aa, bb;
    // basically I have to do the conversion due to the potential usage of double digit years
    if(atext && atext.match(/\S/))
    {
      aa = ml_tsort.convert_date(atext);
      if(isNaN(aa)) aa = Date.parse(atext);
      if(isNaN(aa)) aa = 0;
    }
    else aa = ml_tsort.smallest_int;
    if(btext && btext.match(/\S/))
    {
      bb = ml_tsort.convert_date(btext);
      if(isNaN(bb)) bb = Date.parse(btext);
      if(isNaN(bb)) bb = 0;
    }
    else bb = ml_tsort.smallest_int;
    return aa - bb;
  },
  
  // assume no scientific number in currency (if assumption incorrect, just use
  // same code for this.sort_numeric will do)
  sort_currency : function(a,b) 
  { 
      return ml_tsort.sort_num(ml_tsort.getInnerText(a.cells[ml_tsort.sort_column_index]).replace(/[^-0-9.+]/g,''),
                         ml_tsort.getInnerText(b.cells[ml_tsort.sort_column_index]).replace(/[^-0-9.+]/g,''));
  },
  
  // let's allow scientific notation but also be strict on number format
  sort_num : function(a, b) 
  {
      var aa, bb;
      if(a && a.match(/\S/))
      {
        if(!isNaN(a)) aa = a;
        else if(a && a.match(/^[^0-9.+-]*([+-]?\s*[0-9]+(?:\.[0-9]+)?(?:\s*[eE]\s*[+-]?\s*\d+)?)/))
          aa = parseFloat(RegExp.$1.replace(/\s+/g, ''));
        else aa = 0;
      }
      else aa = ml_tsort.smallest_int;
      if(b && b.match(/\S/))
      {
        if(!isNaN(b)) bb = b;
        else if(b && b.match(/^[^0-9.+-]*([+-]?\s*[0-9]+(?:\.[0-9]+)?(?:\s*[eE]\s*[+-]?\s*\d+)?)/))
          bb = parseFloat(RegExp.$1.replace(/\s+/g, ''));
        else bb = 0;
      }
      else bb = ml_tsort.smallest_int;
      return aa - bb;
  },
  
  sort_numeric : function(a,b) 
  {
      return ml_tsort.sort_num(ml_tsort.getInnerText(a.cells[ml_tsort.sort_column_index]),
                         ml_tsort.getInnerText(b.cells[ml_tsort.sort_column_index])); 
  },
  
  sort_ip : function(a,b) 
  {
      var aa = ml_tsort.getInnerText(a.cells[ml_tsort.sort_column_index]).split('.');
      var bb = ml_tsort.getInnerText(b.cells[ml_tsort.sort_column_index]).split('.');
      return ml_tsort.sort_num(aa[0], bb[0]) || ml_tsort.sort_num(aa[1], bb[1]) || 
             ml_tsort.sort_num(aa[2], bb[2]) || ml_tsort.sort_num(aa[3], bb[3]);
  },
   
  sort_caseinsensitive : function(a,b) 
  {
      var aa = ml_tsort.getInnerText(a.cells[ml_tsort.sort_column_index]).toLowerCase();
      var bb = ml_tsort.getInnerText(b.cells[ml_tsort.sort_column_index]).toLowerCase();
      if (aa==bb) return 0;
      if (aa<bb) return -1;
      return 1;
  },
  
  custom_sortfn : function(aa,bb) 
  {
    var a = ml_tsort.getInnerText(aa.cells[ml_tsort.sort_column_index]);
    var b = ml_tsort.getInnerText(bb.cells[ml_tsort.sort_column_index]);
    return eval(ml_tsort.custom_code);
  },
  
  set_cookie : function(name, value)
  {
  	var date = new Date();
  	var expires = '';
  	if(ml_tsort.cookie_days > 0)
  	{
  	  date.setTime(date.getTime() + ml_tsort.cookie_days * 24 * 360000);
  		expires = '; expires=' + date.toGMTString();
  	}
  	document.cookie = name + '=' + value + expires;
  },
  
  get_cookie : function(name)
  {
    if(document.cookie)
    {
      var st = document.cookie.indexOf(name);
      if (st > -1)
      {
        st = st + name.length + 1; 
        var end = document.cookie.indexOf(';', st);
        if(end == -1) end = document.cookie.length;
        return unescape(document.cookie.substring(st, end));
      }
    }
  }

};

function ml_trim(text)
{
  if(!text) return text;
  var tmp = text.replace(/^\s+/, '');
  return tmp.replace(/\s+$/, '');
}

function ts_addEvent(elm, evType, fn, useCapture)
// addEvent and removeEvent
// cross-browser event handling for IE5+,  NS6 and Mozilla
// By Scott Andrew
{
  if (elm.addEventListener){
    elm.addEventListener(evType, fn, useCapture);
    return true;
  } else if (elm.attachEvent){
    var r = elm.attachEvent("on"+evType, fn);
    return r;
  } else {
    alert("Handler could not be removed");
    return false;
  }
}
ts_addEvent(document, "click", ml_tsort.set_vars);
ts_addEvent(window, "load", ml_tsort.sortables_init);
