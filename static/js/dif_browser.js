
$(document).ready(function() {
    $(document).ajaxStart(function () { $("html").addClass("wait"); });
    $(document).ajaxStop(function () { $("html").removeClass("wait"); });
    $('[data-toggle="popover"]').popover({html: true, delay: { "show": 0, "hide": 2000 }, trigger:"hover"});
    $('[data-toggle="tooltip"]').tooltip('hide');


    var titles = JSON.parse($("#titles").text())
      .map(function(r) { return r.title; })
      .filter(function(t) { return t; });


    var pi_names = JSON.parse($("#pi_names").text())
      .map(function(r) { return r.pi_name; })
      .filter(function(t) { return t; });

    var awards = JSON.parse($("#awards").text())
      .map(function(r) { return r.award; })
      .filter(function(t) { return t; });

    var dif_ids = JSON.parse($("#dif_ids").text())
      .map(function(r) { return r.dif_id; })
      .filter(function(t) { return t; });


    function makeAutocompleteSource(wordlist) {
      return function(term, responseFn) {
          var re = new RegExp($.ui.autocomplete.escapeRegex(term),'i');
          var ret = wordlist.filter(function(t) {return re.test(t); });
          ret.unshift(term);
          return responseFn(ret);
      };
    }
    
    $('[name="title"]').typeahead({
      source: makeAutocompleteSource(titles),
      autoSelect: false
    });

    $('[name="pi_name"]').typeahead({
      source: makeAutocompleteSource(pi_names),
      autoSelect: false
    });

    $('[name="award"]').typeahead({
      source: makeAutocompleteSource(awards),
      autoSelect: false
    });

    $('[name="dif_id"]').typeahead({
      source: makeAutocompleteSource(dif_ids),
      autoSelect: false
    });


  $('#pi_name, #title, #dif_id, #award, #award-input, #dif_id-input, #all_selected, #usap_selected').change(function(e) {
    $('[data-toggle="tooltip"]').tooltip('hide');

    var selected = {
        dif_id: $('#dif_id').val(),
        award: $('#award').val(),
        all_selected: $('#all_selected:checked').val() ? 1 : 0
    };
    updateMenusWithSelected(selected, false);
  });

  $('.abstract-button').click(function(event) {
    var header = $(this).closest('table').find('th');
    var dif_id_ind, abstract_ind = 999;
    for (var i in header) {
      if (header[i].tagName != "TH") continue;
      var label = header[i].innerText;
      if (label == "DIF ID") dif_id_ind = i;
      if (label == "Abstract") abstract_ind = i;
    }
    var row = $(this).closest('tr');
    var dif_id = row.find('td').eq(dif_id_ind).text();
    var abstract = row.find('td').eq(abstract_ind).text();
    $("#abstract_title").text(dif_id);
    $("#abstract_text").text(abstract);
    var x = event.pageX;
    var y = event.pageY;
    $("#abstract").css({top:y-400+"px", left:x-300+"px"}).show();
  });

  $('#close_btn').click(function() {
    $("#abstract").hide();
  });
  
  //Make the DIV element draggagle:
  dragElement(document.getElementById(("abstract")));
});

function updateMenusWithSelected(selected, reset) {
  selected = selected || {};
  return $.ajax({
    method: 'GET',
    url: 'http://' + window.location.hostname + '/filter_dif_menus',
    data: selected,

    success: function(opts) {
      if (reset) {
          document.getElementById("data_link").reset();
          $('#pi_name').text("");
          $('#title').text("");
      }
      console.log(opts);
      for (var menu_name in opts) {
          //console.log('filling opts: ' + menu_name +", " + selected[menu_name]);
          fill_opts(menu_name, opts[menu_name], selected[menu_name]);
      }

      $('[data-toggle="tooltip"]').tooltip({container: 'body'});

    }
  });
} 


function fill_opts(menu_name, opts, selected) {

    var $select = $('#'+menu_name);

    $select.empty();
    switch (menu_name) {
      case 'award':
        $select.append('<option value="">Any award</option>');
        break;
      case 'dif_id':
        $select.append('<option value="">Any DIF ID</option>');
        break;
    }

    for (var opt of opts) {
        if (opt !== '')
          $select.append('<option value="'+opt+'">'+opt+'</option>');
    }

    switch (menu_name) {
      case 'award':
        var val = selected ? selected : "Any award";
        $('#award-input').val(val);
        $('#award option[value="'+val+'"]').prop('selected', true);
        break;
      case 'dif_id':
          val = selected ? selected : "Any DIF ID";
          $('#dif_id-input').val(val);
          $('#dif_id option[value="'+val+'"]').prop('selected', true);
          break;
    }

}

/*
  functions to hangle dragging an element by its header
*/
function dragElement(elmnt) {
  if (elmnt === null) return;
  var pos1 = 0, pos2 = 0, pos3 = 0, pos4 = 0;
  if (document.getElementById(elmnt.id + "_header")) {
    /* if present, the header is where you move the DIV from:*/
    document.getElementById(elmnt.id + "_header").onmousedown = dragMouseDown;
  } else {
    /* otherwise, move the DIV from anywhere inside the DIV:*/
    elmnt.onmousedown = dragMouseDown;
  }

  function dragMouseDown(e) {
    e = e || window.event;
    // get the mouse cursor position at startup:
    pos3 = e.clientX;
    pos4 = e.clientY;
    document.onmouseup = closeDragElement;
    // call a function whenever the cursor moves:
    document.onmousemove = elementDrag;
  }

  function elementDrag(e) {
    e = e || window.event;
    // calculate the new cursor position:
    pos1 = pos3 - e.clientX;
    pos2 = pos4 - e.clientY;
    pos3 = e.clientX;
    pos4 = e.clientY;
    // set the element's new position:
    elmnt.style.top = (elmnt.offsetTop - pos2) + "px";
    elmnt.style.left = (elmnt.offsetLeft - pos1) + "px";
  }

  function closeDragElement() {
    /* stop moving when mouse button is released:*/
    document.onmouseup = null;
    document.onmousemove = null;
  }
}
