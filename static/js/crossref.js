$(document).ready(function() {
  // display crossref button if an award has been selected
  for (var a of $("input.award_input:hidden")) {
    var award = a.value;
    if (award != '' && award != 'Not_In_This_List:' && award != 'None') {
      $("#crossref_btn").show();
    }
  }


  //handle crossref botton click
  $('#crossref_btn').click(function(event) {
    //get all awards
    var awards = [];
    for (var a of $("input.award_input:hidden")) {
      if (a.value) {
        var award_num = a.value.split(' ')[0];
        if (award_num !== "None") {
          awards.push(award_num);
        }
      }
    }

    if (awards.length != 0) {
      $("#crossref_title").text('Publications found on Crossref');
      $("#crossref_body").text('Please Wait...');
      $("#close_crossref_btn").text('Close').show();
      var x = event.pageX;
      var y = event.pageY;
      //display crossref popup
      $("#crossref").css({top:y-350+"px", left:"100px"}).show();
      //get list of publications
      getPublicationsFromCrossref(awards)
    }
        
      
  });


  $('#crossref_x_btn').click(function() {
    $("#crossref_body").empty();
    $("#crossref").hide();
  });

  //Make the DIV element draggagle:
  dragElement(document.getElementById(("crossref")));
  
});


function getPublicationsFromCrossref(awards) {
  var pubs = []
  $.ajax({
    url: window.location.protocol + '//' + window.location.hostname + '/crossref_pubs',
    method: 'POST',
    data: {awards:awards},
    dataType: "json",
    success: function(msg) {
      $("#crossref_body").empty();

      if (msg.length === 0) {
        $("#crossref_body").append($('<div/>', {'class' : 'extraLocation', html: "No publications found in Crossref for this award"}))
      }
      else {
        // get list of publications doi's already entered on the form
        current_pubs = []
        $(".pub_doi").each(function() {
          current_pubs.push($(this).val());
        });
        console.log(current_pubs)
        for (var pub of msg) {
          // add a line of html
          var line = $('<div/>') ;
          var input = $('<input/>', {type: 'checkbox', class: 'crossref_pubs', name: pub.ref_text, id:pub.doi})
          line.append(input);
          line.append('<label for="'+pub.doi+'">&nbsp;'+pub.doi+'</label>');
          line.append('<br>');
          line.append(pub.ref_text);
          line.append('<br><br>');

          // if doi is doi is already listed in submission, grey out the line
          if (current_pubs.includes(pub.doi)) {
            line.addClass('disabled');
            input.attr("disabled", true);
          }
          $("#crossref_body").append(line);
        }
        $("#close_crossref_btn").text("Add publications");
      }

    },
    error: function(jqXHR, textStatus, errorThrown) {
      console.log(textStatus)
      console.log(errorThrown)
      $("#crossref_body").append($('<div/>', {'class' : 'extraLocation', html: "No publications found in Crossref for this award"}))
    }
  })

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