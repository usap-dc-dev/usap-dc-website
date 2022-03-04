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

    var main_award = {id:$("#main_award").text(), is_previous_award: false};
    var awards = [];
    if ($("#awards_list").text().length > 0) {
      awards = JSON.parse($("#awards_list").text());
    }

    var copis = [];
    if ($("#copis_list").text().length > 0) {
      copis = JSON.parse($("#copis_list").text());
    }

    var websites = [];
    if ($("#websites_list").text().length > 0) {
      websites = JSON.parse($("#websites_list").text());
    }

    var deployments = [];
    if ($("#deployments_list").text().length > 0) {
      deployments = JSON.parse($("#deployments_list").text());
    }

    var locations = [];
    if ($("#locations_list").text().length > 0) {
      locations = JSON.parse($("#locations_list").text());
    }

    var parameters = [];
    if ($("#parameters_list").text().length > 0) {
      parameters = JSON.parse($("#parameters_list").text());
    }

    var publications = [];
    if ($("#publications_list").text().length > 0) {
      publications = JSON.parse($("#publications_list").text());
    }

    var datasets = [];
    if ($("#datasets_list").text().length > 0) {
      datasets = JSON.parse($("#datasets_list").text());
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
    var location_wrapper = $('#more_locations');
    var location_counter = 1;


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

    $('.dropdown').each(function(i,elem) {$(elem).makeDropdownIntoSelect('',''); });

    $('[data-toggle="tooltip"]').tooltip({container: 'body'}); 

    $('#close_crossref_btn').click(function() {
      $(".crossref_pubs").each(function() {
        if ($(this).prop('checked')) {
          var doi = $(this).attr('id');
          var name = $(this).attr('name');
          addPubRow({doi:doi, name:name});
        } 
      });
      $("#crossref_body").empty();
      $("#crossref").hide();
    });

    $('#award').change(function() {
      var title = pi = institution = email = copi = start = end = cr = ipy = null;
      var val = $('#award').val();
      if (val == "Not_In_This_List") {
        $(this).parent('div').find('.user_award_input').attr('type','text');
      } else {
        $(this).parent('div').find('.user_award_input').attr('type','hidden');
      }

      if (val != '' && val != 'Not In This List' && val != 'None') {
        var award_num = val.split(' ')[0];
        $("#crossref_btn").show();
        $.ajax({
        method: 'GET',
        url: window.location.protocol + '//' + window.location.hostname + '/submit/projectinfo?award='+award_num,
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
                    $('#copi_name_first').attr('value', copi[1].trim());
                
                    for (var i=1; i < copis.length; i++ ) {
                        copi = copis[i].split(',');
                        var author = {'last_name': copi[0], 'first_name': copi[1].trim()};
                        addAuthorRow(author);
                    }
                }
            }
            $("#entry textarea[name='title']").val(msg.title);
            $("#entry input[name='pi_name_last']").val(pi[0]);
            $("#entry input[name='pi_name_first']").val(pi[1].trim());
            $("#entry input[name='org']").val(msg.org);
            if (msg.email) $("#entry input[name='email']").val(msg.email);
            $("#entry input[name='copi']").val(msg.copi);
            $("#entry input[name='start']").val(to_yyyymmdd(msg.start));
            $("#entry input[name='end']").val(to_yyyymmdd(msg.expiry));
            $("#entry input[name='iscr']").prop('checked',msg.iscr);
            $("#entry input[name='isipy']").prop('checked',msg.isipy);
            $("#entry textarea[name='sum']").val(msg.sum);

            if (msg.proj_uid) {
              var proj_url = window.location.protocol + '//' + window.location.hostname + '/view/project/' + msg.proj_uid;
              var alert = "A project already exists for this award at<br><a href='" + proj_url + "' target='_blank'>" + proj_url + "</a>.<br>You can edit this project if necessary."
              $("#proj_alert_msg").html(alert);
              $("#proj_alert").show();

            }
        }
        });
      } else {
        $("#crossref_btn").hide();
      }

      //if in Edit mode and the main award changes, add the old award an an Additional award and mark as Previous
      if (awards.length > 0) {
        main_award.is_previous_award = true;
        if (!awards.includes(main_award)){
          awards.push(main_award);
          addAwardRow(main_award);
          //make title and abstract editable
          $("#title").attr('readonly', false); 
          $("#sum").attr('readonly', false); 
        }
      }
    });

    $('.close_proj_alert_btn').click(function() {
      $("#proj_alert").hide();
    });

    //Make the DIV element draggagle:
    dragElement(document.getElementById(("proj_alert")));

  // if reloading, populate any added location fields
  $('#location').attr('value', locations[0]);
  for (i=1; i < locations.length; i++ ) {
    var location = locations[i];
    addLocationRow(location);
  }

  $('#addLocationRow').click(function (e) {
      //code to add another file
      e.preventDefault();
      addLocationRow();
    });

  $(location_wrapper).on("click","#removeLocationRow", function(e){ //user click on remove field
    e.preventDefault(); 
    $(this).parent('div').parent('div').remove();
    location_counter--;
  });


  $(".location_input").on("change", function() {
      if ($(this).val() == "Not In This List") {
        $(this).parent('div').find('.user_location_input').attr('type','text');
      } else {
        $(this).parent('div').find('.user_location_input').attr('type','hidden');
      }
    });

  function addLocationRow(location) {
    var extraLocation = $('.locationTemplate').clone();
      //increment the element ids
      $(extraLocation).find('#location-dropdown').attr({'id': 'location-dropdown'+location_counter});
      $(extraLocation).find('#location').attr({'id': 'location'+location_counter, 'name': 'location'+location_counter, 'value': ''});
      $(extraLocation).find('#location_dropdown').attr({'id': 'location_dropdown'+location_counter}).html('None <span class="caret"></span>');
      $(extraLocation).find('#user_location').attr({'id': 'user_location'+location_counter, 'name': 'user_location'+location_counter, 'type':'hidden', 'value': ''});
      $(extraLocation).find('#removeLocationRow').show();
      $(extraLocation).find('#extraLocationLine').show();
      if (location !== null) {
        $(extraLocation).find('#location'+location_counter).attr('value', location);
      }
      $(location_wrapper).append($('<div/>', {'class' : 'extraLocation', html: extraLocation.html()}));
      location_counter++;

      $('.dropdown').each(function(i,elem) {$(elem).makeDropdownIntoSelect('',''); });
      $('[data-toggle="tooltip"]').tooltip({container: 'body'});

      $(".location_input").on("change", function() {
          if ($(this).val() == "Not In This List") {
            $(this).parent('div').find('.user_location_input').attr('type','text');
          } else {
            $(this).parent('div').find('.user_location_input').attr('type','hidden');
          }
      });
    }


    // if editing, populate any added parameter fields
    if (parameters.length > 0) {
      $('#parameter1').val(parameters[0]);
      
      for (i=1; i < parameters.length; i++ ) {
        addParameter(parameters[i]);
      }
    }

    $('#add-parameter').click(function(e) {
      e.preventDefault();
      addParameter();
    });

    function addParameter(parameter) {
      var $cloned = $("#parameters select[name='parameter1']").clone();
      var idx = $('#parameters').children().length+1;
      $cloned.attr('name','parameter'+idx);
      $cloned.attr('id', 'parameter'+idx);
      $cloned.removeAttr('required');
      $cloned.children().first().text('(Select Term ' + idx + ')');
      $('#parameters').append($cloned); 
 
      if (typeof parameter != 'undefined') {
        $('#parameter'+idx).val(parameter);
      }  
    }

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

    // if editing, populate any added copi fields
    if (copis.length > 0) {
      var copi = copis[0];
      $('#copi_name_last').val(copi.name_last);
      $('#copi_name_first').val(copi.name_first);
      $('#copi_org').val(copi.org);
      $('#copi_role').val(copi.role);

      for (i=1; i < copis.length; i++ ) {
        copi = copis[i];
        addAuthorRow(copi);
      }
    }

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

          $(author_wrapper).append($('<div/>', {'class' : 'extraAuthor', html: extraAuthor.html()}));

          if (typeof author != 'undefined') {
            $('#copi_name_last'+author_counter).val(author.name_last);
            $('#copi_name_first'+author_counter).val(author.name_first);
            $('#copi_org'+author_counter).val(author.org);
            $('#copi_role'+author_counter).val(author.role);
          }

          autocomplete(document.getElementById("copi_name_last"+author_counter), document.getElementById("copi_name_first"+author_counter), persons);
          autocomplete(document.getElementById("copi_org"+author_counter), null, orgs);
          author_counter++;
    }

  
  // if editing, populate any added datasets fields
  if (datasets.length > 0) {
    var dataset = datasets[0];
    $('#ds_repo').val(dataset.repository);
    $('#ds_title').val(dataset.title);
    $('#ds_url').val(dataset.url);
    $('#ds_doi').val(dataset.doi);
  
    for (i=1; i < datasets.length; i++ ) {
      addDatasetRow(datasets[i]);
    }
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

  function addDatasetRow(dataset) {
    var extraDataset = $('.datasetTemplate').clone();
    //increment the element ids
    $(extraDataset).find('#ds_repo').attr({'id': 'ds_repo'+ds_counter, 'name': 'ds_repo'+ds_counter, 'value': ''});
    $(extraDataset).find('#ds_title').attr('id', 'ds_title'+ds_counter).attr('name', 'ds_title'+ds_counter).html('');
    $(extraDataset).find('#ds_url').attr('id', 'ds_url'+ds_counter).attr('name', 'ds_url'+ds_counter).html('');
    $(extraDataset).find('#ds_doi').attr({'id': 'ds_doi'+ds_counter, 'name': 'ds_doi'+ds_counter, 'value': ''});
    $(extraDataset).find('#removeDatasetRow').show();
    $(extraDataset).find('#extraDatasetLine').show();
    $(ds_wrapper).append($('<div/>', {'class' : 'extraDataset', html: extraDataset.html()}));

    if (typeof dataset != 'undefined') {
      $('#ds_repo'+ds_counter).val(dataset.repository);
      $('#ds_title'+ds_counter).val(dataset.title);
      $('#ds_url'+ds_counter).val(dataset.url);
      $('#ds_doi'+ds_counter).val(dataset.doi);
    }
    ds_counter++;
  }

  
  // if editing, populate any added publication fields
  if (publications.length > 0) {  
    for (i=0; i < publications.length; i++ ) {
      addPubRow(publications[i]);
    }
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
      if (publication && $('#publication').val() === "") {
        $('#publication').html(publication.name);
        $('#pub_doi').val(publication.doi);
      }
      else {
        var extraPub = $('.publicationTemplate').clone();
        //increment the element ids
        $(extraPub).find('#publication').attr('id', 'publication'+pub_counter).attr('name', 'publication'+pub_counter).html('');
        $(extraPub).find('#pub_doi').attr({'id': 'pub_doi'+pub_counter, 'name': 'pub_doi'+pub_counter, 'value': ''});
        $(extraPub).find('#removePubRow').show();
        $(extraPub).find('#extraPubLine').show();
        if (typeof publication != 'undefined') {
            $(extraPub).find('#publication'+pub_counter).html(publication.name);
            $(extraPub).find('#pub_doi'+pub_counter).attr('value', publication.doi);
        }
        $(pub_wrapper).append($('<div/>', {'class' : 'extraPub', html: extraPub.html()}));
        pub_counter++;
      }   
  }

  // if editing, populate any added award fields
  for (i=0; i < awards.length; i++ ) {
    var award = awards[i];
    addAwardRow(award);
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

  $(".award_input").on("change", function() {
      if ($(this).val() == "Not In This List") {
        $(this).parent('div').find('.user_award_input').attr('type','text');
      } else {
        $(this).parent('div').find('.user_award_input').attr('type','hidden');
      }
    });

  function addAwardRow(award) {
    var extraAward = $('.awardTemplate').clone();
      //increment the element ids
      $(extraAward).find('#award-dropdown').attr({'id': 'award-dropdown'+award_counter});
      $(extraAward).find('#award').attr({'id': 'award'+award_counter, 'name': 'award'+award_counter, 'value': ''});
      $(extraAward).find('#award_dropdown').attr({'id': 'award_dropdown'+award_counter}).html('None <span class="caret"></span>');
      $(extraAward).find('#user_award').attr({'id': 'user_award'+award_counter, 'name': 'user_award'+award_counter, 'type':'hidden', 'value': ''});
      $(extraAward).find('#previous_award').attr({'id': 'previous_award'+award_counter, 'name': 'previous_award'+award_counter});
      $(extraAward).find('#previous_award_div').show();
      $(extraAward).find('#removeAwardRow').show();
      $(extraAward).find('#extraAwardLine').show();
      if (award) {
        $(extraAward).find('#award'+award_counter).attr('value', award.id);
        $(extraAward).find('#previous_award'+award_counter).attr('checked', award.is_previous_award)
        // $(extraAward).find('#user_award'+award_counter).attr('value', award.user_award);
      }
      $(award_wrapper).append($('<div/>', {'class' : 'extraAward', html: extraAward.html()}));
      award_counter++;

      $('.dropdown').each(function(i,elem) {$(elem).makeDropdownIntoSelect('',''); });
      $('[data-toggle="tooltip"]').tooltip({container: 'body'});

      $(".award_input").on("change", function() {
          if ($(this).val() == "Not In This List") {
            $(this).parent('div').find('.user_award_input').attr('type','text');
          } else {
            $(this).parent('div').find('.user_award_input').attr('type','hidden');
            $("#crossref_btn").show();
          }
      });
    }


    // if editing, populate any added website fields
    if (websites.length > 0) {
      $('#website_title').val(websites[0].title);
      $('#website_url').val(websites[0].url);
      
      for (i=1; i < websites.length; i++ ) {
        addWebsiteRow(websites[i]);
      }
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

    function addWebsiteRow(website) {
      var extraWebsite = $('.websiteTemplate').clone();
      //increment the element ids
      $(extraWebsite).find('#website_title').attr({'id': 'website_title'+website_counter, 'name': 'website_title'+website_counter, 'value': ''});
      $(extraWebsite).find('#website_url').attr('id', 'website_url'+website_counter).attr('name', 'website_url'+website_counter).html('');
      $(extraWebsite).find('#removeWebsiteRow').show();
      $(extraWebsite).find('#extraWebsiteLine').show();
      $(website_wrapper).append($('<div/>', {'class' : 'extraWebsite', html: extraWebsite.html()}));

      if (typeof website != 'undefined') {
        $('#website_title'+website_counter).val(website.title);
        $('#website_url'+website_counter).val(website.url);
      }
      website_counter++;
    }


    // if editing, populate any added deployment fields
    if (deployments.length > 0) {
      $('#deployment_name').val(deployments[0].name);
      $('#deployment_type').val(deployments[0].type);
      $('#deployment_url').val(deployments[0].url);
      
      for (i=1; i < deployments.length; i++ ) {
        addDeploymentRow(deployments[i]);
      }
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

    function addDeploymentRow(deployment) {
      var extraDeployment = $('.deploymentTemplate').clone();
      //increment the element ids
      $(extraDeployment).find('#deployment_name').attr({'id': 'deployment_name'+deployment_counter, 'name': 'deployment_name'+deployment_counter, 'value': ''});
      $(extraDeployment).find('#deployment_type').attr('id', 'deployment_type'+deployment_counter).attr('name', 'deployment_type'+deployment_counter).html('');
      $('#deployment_type').find('option').clone().appendTo($(extraDeployment).find('#deployment_type'+deployment_counter));
      $(extraDeployment).find('#deployment_url').attr('id', 'deployment_url'+deployment_counter).attr('name', 'deployment_url'+deployment_counter).html('');
      $(extraDeployment).find('#removeDeploymentRow').show();
      $(extraDeployment).find('#extraDeploymentLine').show();
      $(deployment_wrapper).append($('<div/>', {'class' : 'extraDeployment', html: extraDeployment.html()}));
      
      if (typeof deployment != 'undefined') {
        $('#deployment_name'+deployment_counter).val(deployment.name);
        $('#deployment_type'+deployment_counter).val(deployment.type);
        $('#deployment_url'+deployment_counter).val(deployment.url);
      }

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
            b.innerHTML += '<input type="hidden" value="' + arr[i] + '">';
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
