to_yyyymmdd = function(date_in) {
  var date = new Date(date_in);
  var mm = date.getUTCMonth() + 1; // getUTCMonth() is zero-based
  var dd = date.getUTCDate();

  return [date.getUTCFullYear(),
          (mm>9 ? '' : '0') + mm,
          (dd>9 ? '' : '0') + dd
         ].join('-');
};



$(document).ready(function() {
    var persons_rows = JSON.parse($("#persons").text());
    var persons = [];
    for (var r in persons_rows) {
        row = persons_rows[r];
        persons.push(row.id);
    }

    var orgs_rows = JSON.parse($("#orgs").text());
    var orgs = [];
    for (r in orgs_rows) {
      org = orgs_rows[r];
      orgs.push(org.name);
    }

    /*initiate the autocomplete function on the "author" elements, and pass along the persons array as possible autocomplete values:*/
    autocomplete(document.getElementById("copi_name_last"), document.getElementById("copi_name_first"), persons);
    autocomplete(document.getElementById("pi_name_last"), document.getElementById("pi_name_first"), persons);
    autocomplete(document.getElementById("org"), null, orgs);
    autocomplete(document.getElementById("copi_org"), null, orgs);

    var author_wrapper = $('#more_authors');
    var author_counter = 1;
    var ds_wrapper = $('#more_datasets');
    var ds_counter = 1;
    var pub_wrapper = $('#more_publications');
    var pub_counter = 1;
    var award_wrapper = $('#more_awards');
    var award_counter = 1;
    var website_wrapper = $('#more_websites');
    var website_counter = 1;
    var deployment_wrapper = $('#more_deployments');
    var deployment_counter = 1;


    var check = $('input[name="entire_region"]');
    var w = $('input[name="geo_w"]');
    var e = $('input[name="geo_e"]');
    var s = $('input[name="geo_s"]');
    var n = $('input[name="geo_n"]');
    check.on('click', function() {
      if (check.prop('checked')) {
        w.val('-180');
        e.val('180');
        s.val('-90');
        n.val('-60');
      } else {
        w.val('');
        e.val('');
        s.val('');
        n.val('');
      }
    });

    $('#award').change(function() {
      var title = pi = institution = email = copi = start = end = cr = ipy = null;
      var val = $('#award').val();
      if (val != '0') {
        var award_num = val.split(' ')[0];
        $.ajax({
        method: 'GET',
        url: 'http://' + window.location.hostname + '/submit/projectinfo?award='+award_num,
        success: function(msg) {
            pi = msg.name.split(',');
            //reset co-pis
            $('#copi_name_last').attr('value', '');
            $('#copi_name_first').attr('value', '');
            $('#more_authors').empty();
            author_counter = 1;


            // add co-pis
            if (msg.copi != null && msg.copi !== ''){
                var copis = msg.copi.split(';');
                if (copis.length > 0) {
                    copi = copis[0].split(',');
                    $('#copi_name_last').attr('value', copi[0]);
                    $('#copi_name_first').attr('value', copi[1]);
                
                    for (var i=1; i < copis.length; i++ ) {
                        copi = copis[i].split(',');
                        var author = {'last_name': copi[0], 'first_name': copi[1]};
                        addAuthorRow(author);
                    }
                }
            }
            $("#entry textarea[name='title']").val(msg.title);
            $("#entry input[name='pi_name_last']").val(pi[0]);
            $("#entry input[name='pi_name_first']").val(pi[1]);
            $("#entry input[name='org']").val(msg.org);
            $("#entry input[name='email']").val(msg.email);
            $("#entry input[name='copi']").val(msg.copi);
            $("#entry input[name='start']").val(to_yyyymmdd(msg.start));
            $("#entry input[name='end']").val(to_yyyymmdd(msg.expiry));
            $("#entry input[name='iscr']").prop('checked',msg.iscr);
            $("#entry input[name='isipy']").prop('checked',msg.isipy);
            $("#entry textarea[name='sum']").val(msg.sum);
        }
        });
      }
    
    });

    $('#add-location').click(function() {
      var $cloned = $("#locations select[name='location1']").clone();
      var idx = $('#locations').children().length+1;
      $cloned.attr('name','location'+idx);
      $cloned.attr('id', 'location'+idx);
      $cloned.removeAttr('required');
      $cloned.children().first().text('(Select Term ' + idx + ')');
      $('#locations').append($cloned);
    
    });

    $('#add-parameter').click(function() {
      var $cloned = $("#parameters select[name='parameter1']").clone();
      var idx = $('#parameters').children().length+1;
      $cloned.attr('name','parameter'+idx);
      $cloned.attr('id', 'parameter'+idx);
      $cloned.removeAttr('required');
      $cloned.children().first().text('(Select Term ' + idx + ')');
      $('#parameters').append($cloned);   
    });

    $('#add_repo').click(function() {
      var repos = $('#repositories');
      var idx = repos.length+1;
      var new_repo = $('#repo1').clone();
      new_repo.attr('id', 'repo'+idx);
      new_repo.find('input[name="repo1"]').attr('name','repo'+idx).prop('checked', false);
      new_repo.find('input[name="repo_name_other1"]').attr('name','repo_name_other'+idx);
      new_repo.find('input[name="repo_id_other1"]').attr('name', 'repo_id_other'+idx);
      repos.append('<hr>');
      repos.append(new_repo);
      });

    $('input[name="repos"]').change(function() {
    if ($(this).prop('checked')) {
        $("#repositories input[type='radio']").prop('checked', false);
        $("#repositories input[type='text']").val('');
    }
    });

    $("#repositories input[type='radio']").change(function() {
    if ($(this).prop('checked')) {
        $('input[name="repos"]').prop('checked', false);
    }
    });


    $('#addAuthorRow').click(function (e) {
          //code to add another author
          e.preventDefault();
          addAuthorRow();
    });

    $(author_wrapper).on("click","#removeAuthorRow", function(e){ //user click on remove field
          e.preventDefault(); 
          $(this).parent().closest('div').remove();
          author_counter--;
    });

    function addAuthorRow(author) {
          var extraAuthor = $('.authorTemplate').clone();
          //increment the element ids
          $(extraAuthor).find('#copi_name_last').attr({'id': 'copi_name_last'+author_counter, 'name': 'copi_name_last'+author_counter, 'value': ''});
          $(extraAuthor).find('#copi_name_first').attr({'id': 'copi_name_first'+author_counter, 'name': 'copi_name_first'+author_counter, 'value': ''});
          $(extraAuthor).find('#copi_role').attr({'id': 'copi_role'+author_counter, 'name': 'copi_role'+author_counter, 'value': ''});
          $(extraAuthor).find('#copi_org').attr({'id': 'copi_org'+author_counter, 'name': 'copi_org'+author_counter, 'value': ''});
          $(extraAuthor).find('#removeAuthorRow').show();
          $(extraAuthor).find('#extraAuthorLine').show();
          if (typeof author != 'undefined') {
           $(extraAuthor).find('#copi_name_last'+author_counter).attr('value', author.last_name);
           $(extraAuthor).find('#copi_name_first'+author_counter).attr('value', author.first_name);
          }
          $(author_wrapper).append($('<div/>', {'class' : 'extraAuthor', html: extraAuthor.html()}));
          autocomplete(document.getElementById("copi_name_last"+author_counter), document.getElementById("copi_name_first"+author_counter), persons);
          autocomplete(document.getElementById("copi_org"+author_counter), null, orgs);
          author_counter++;
    }

    $('#addDatasetRow').click(function (e) {
      //code to add another dataset
      e.preventDefault();
      addDatasetRow();
    });

    $(ds_wrapper).on("click","#removeDatasetRow", function(e){ //user click on remove field
      e.preventDefault(); 
      $(this).parent().closest('div').remove();
      ds_counter--;
    });

    function addDatasetRow() {
      var extraDataset = $('.datasetTemplate').clone();
      //increment the element ids
      $(extraDataset).find('#ds_repo').attr({'id': 'ds_repo'+ds_counter, 'name': 'ds_repo'+ds_counter, 'value': ''});
      $(extraDataset).find('#ds_title').attr('id', 'ds_title'+ds_counter).attr('name', 'ds_title'+ds_counter).html('');
      $(extraDataset).find('#ds_url').attr('id', 'ds_url'+ds_counter).attr('name', 'ds_url'+ds_counter).html('');
      $(extraDataset).find('#ds_doi').attr({'id': 'ds_doi'+ds_counter, 'name': 'ds_doi'+ds_counter, 'value': ''});
      $(extraDataset).find('#removeDatasetRow').show();
      $(extraDataset).find('#extraDatasetLine').show();
      // if (typeof publication != 'undefined') {
      //     $(extraPub).find('#publication'+pub_counter).html(publication.text);
      //     $(extraPub).find('#pub_doi'+pub_counter).attr('value', publication.doi);
      // }
      $(ds_wrapper).append($('<div/>', {'class' : 'extraDataset', html: extraDataset.html()}));
      ds_counter++;
    }

  
  $('#addPubRow').click(function (e) {
      //code to add another file
      e.preventDefault();
      addPubRow();
  });

  $(pub_wrapper).on("click","#removePubRow", function(e){ //user click on remove field
      e.preventDefault(); 
      $(this).parent('div').remove();
      pub_counter--;
  });

  function addPubRow(publication) {
      var extraPub = $('.publicationTemplate').clone();
      //increment the element ids
      $(extraPub).find('#publication').attr('id', 'publication'+pub_counter).attr('name', 'publication'+pub_counter).html('');
      $(extraPub).find('#pub_doi').attr({'id': 'pub_doi'+pub_counter, 'name': 'pub_doi'+pub_counter, 'value': ''});
      $(extraPub).find('#removePubRow').show();
      $(extraPub).find('#extraPubLine').show();
      if (typeof publication != 'undefined') {
          $(extraPub).find('#publication'+pub_counter).html(publication.text);
          $(extraPub).find('#pub_doi'+pub_counter).attr('value', publication.doi);
      }
      $(pub_wrapper).append($('<div/>', {'class' : 'extraPub', html: extraPub.html()}));
      pub_counter++;
  }


  $('#addAwardRow').click(function (e) {
      //code to add another file
      e.preventDefault();
      addAwardRow();
  });

  $(award_wrapper).on("click","#removeAwardRow", function(e){ //user click on remove field
      e.preventDefault(); 
      $(this).parent('div').remove();
      award_counter--;
  });

  function addAwardRow() {
      var extraAward = $('.awardTemplate').clone();
      //increment the element ids
      $(extraAward).find('#award').attr('id', 'award'+award_counter).attr('name', 'award'+award_counter).html('').removeAttr('required');
      $('#award').find('option').clone().appendTo($(extraAward).find('#award'+award_counter));
      $(extraAward).find('#removeAwardRow').show();
      $(extraAward).find('#extraAwardLine').show();
      $(award_wrapper).append($('<div/>', {'class' : 'extraAward', html: extraAward.html()}));
      award_counter++;
  }


    $('#addWebsiteRow').click(function (e) {
      //code to add another dataset
      e.preventDefault();
      addWebsiteRow();
    });

    $(website_wrapper).on("click","#removeWebsiteRow", function(e){ //user click on remove field
      e.preventDefault(); 
      $(this).parent().closest('div').remove();
      website_counter--;
    });

    function addWebsiteRow() {
      var extraWebsite = $('.websiteTemplate').clone();
      //increment the element ids
      $(extraWebsite).find('#website_title').attr({'id': 'website_title'+website_counter, 'name': 'website_title'+website_counter, 'value': ''});
      $(extraWebsite).find('#website_url').attr('id', 'website_url'+website_counter).attr('name', 'website_url'+website_counter).html('');
      $(extraWebsite).find('#removeWebsiteRow').show();
      $(extraWebsite).find('#extraWebsiteLine').show();
      $(website_wrapper).append($('<div/>', {'class' : 'extraWebsite', html: extraWebsite.html()}));
      website_counter++;
    }


    $('#addDeploymentRow').click(function (e) {
      if ($(this).hasClass('disabled')) return;
      //code to add another dataset
      e.preventDefault();
      addDeploymentRow();
    });

    $(deployment_wrapper).on("click","#removeDeploymentRow", function(e){ //user click on remove field
      e.preventDefault(); 
      $(this).parent().closest('div').remove();
      deployment_counter--;
    });

    function addDeploymentRow() {
      var extraDeployment = $('.deploymentTemplate').clone();
      //increment the element ids
      $(extraDeployment).find('#deployment_name').attr({'id': 'deployment_name'+deployment_counter, 'name': 'deployment_name'+deployment_counter, 'value': ''});
      $(extraDeployment).find('#deployment_type').attr('id', 'deployment_type'+deployment_counter).attr('name', 'deployment_type'+deployment_counter).html('');
      $('#deployment_type').find('option').clone().appendTo($(extraDeployment).find('#deployment_type'+deployment_counter));
      $(extraDeployment).find('#deployment_url').attr('id', 'deployment_url'+deployment_counter).attr('name', 'deployment_url'+deployment_counter).html('');
      $(extraDeployment).find('#removeDeploymentRow').show();
      $(extraDeployment).find('#extraDeploymentLine').show();
      $(deployment_wrapper).append($('<div/>', {'class' : 'extraDeployment', html: extraDeployment.html()}));
      deployment_counter++;
    }

});

function autocomplete(inp, inp2, arr) {
  /*the autocomplete function takes three arguments,
  two text field elements and an array of possible autocompleted values:*/
  var currentFocus;
  /*execute a function when someone writes in the text field:*/
  [inp, inp2].forEach(function(elem) {
    if (elem === null) return;
    elem.addEventListener("input", function(e) {
        var a, b, i, val = this.value;
        /*close any already open lists of autocompleted values*/
        closeAllLists();
        if (!val) { return false;}
        currentFocus = -1;
        /*create a DIV element that will contain the items (values):*/
        a = document.createElement("DIV");
        a.setAttribute("id", this.id + "autocomplete-list");
        a.setAttribute("class", "autocomplete-items");
        /*append the DIV element as a child of the autocomplete container:*/
        this.parentNode.appendChild(a);
        /*for each item in the array...*/
        for (i = 0; i < arr.length; i++) {
          /*check if the item starts with the same letters as the text field value:*/
          if (arr[i].toUpperCase().indexOf(val.toUpperCase()) != -1) {
            /*create a DIV element for each matching element:*/
            b = document.createElement("DIV");
            /*make the matching letters bold:*/
            b.innerHTML = "<strong>" + arr[i].substr(0, val.length) + "</strong>";
            b.innerHTML += arr[i].substr(val.length);
            /*insert a input field that will hold the current array item's value:*/
            b.innerHTML += "<input type='hidden' value='" + arr[i] + "'>";
            /*execute a function when someone clicks on the item value (DIV element):*/
            b.addEventListener("click", function(e) {
                var selected;
                if (inp2 !== null) {
                  /*insert the value for the autocomplete text field:*/
                  //split the name at the comma
                  selected = this.getElementsByTagName("input")[0].value.split(',');
                  inp.value = selected[0].trim();
                  inp2.value = selected[1].trim();
                }
                else {
                  selected = this.getElementsByTagName("input")[0].value;
                  inp.value = selected;
                }
                /*close the list of autocompleted values,
                (or any other open lists of autocompleted values:*/
                closeAllLists();
            });
            a.appendChild(b);
          }
        }
    });

    /*execute a function presses a key on the keyboard:*/
    elem.addEventListener("keydown", function(e) {
        var x = document.getElementById(this.id + "autocomplete-list");
        if (x) x = x.getElementsByTagName("div");
        if (e.keyCode == 40) {
          /*If the arrow DOWN key is pressed,
          increase the currentFocus variable:*/
          currentFocus++;
          /*and and make the current item more visible:*/
          addActive(x);
        } else if (e.keyCode == 38) { //up
          /*If the arrow UP key is pressed,
          decrease the currentFocus variable:*/
          currentFocus--;
          /*and and make the current item more visible:*/
          addActive(x);
        } else if (e.keyCode == 13) {
          /*If the ENTER key is pressed, prevent the form from being submitted,*/
          e.preventDefault();
          if (currentFocus > -1) {
            /*and simulate a click on the "active" item:*/
            if (x) x[currentFocus].click();
          }
        }
    });
  });

  function addActive(x) {
    /*a function to classify an item as "active":*/
    if (!x) return false;
    /*start by removing the "active" class on all items:*/
    removeActive(x);
    if (currentFocus >= x.length) currentFocus = 0;
    if (currentFocus < 0) currentFocus = (x.length - 1);
    /*add class "autocomplete-active":*/
    x[currentFocus].classList.add("autocomplete-active");
  }
  function removeActive(x) {
    /*a function to remove the "active" class from all autocomplete items:*/
    for (var i = 0; i < x.length; i++) {
      x[i].classList.remove("autocomplete-active");
    }
  }
  function closeAllLists(elmnt) {
    /*close all autocomplete lists in the document,
    except the one passed as an argument:*/
    var x = document.getElementsByClassName("autocomplete-items");
    for (var i = 0; i < x.length; i++) {
      if (elmnt != x[i] && elmnt != inp) {
        x[i].parentNode.removeChild(x[i]);
      }
    }
  }
  /*execute a function when someone clicks in the document:*/
  document.addEventListener("click", function (e) {
      closeAllLists(e.target);
  });
}



