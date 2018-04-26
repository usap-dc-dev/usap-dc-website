
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

});
 