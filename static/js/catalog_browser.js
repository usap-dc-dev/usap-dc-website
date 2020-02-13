
$(document).ready(function() {
    $(document).ajaxStart(function () { $("html").addClass("wait"); });
    $(document).ajaxStop(function () { $("html").removeClass("wait"); });
    // $('[data-toggle="popover"]').popover({html: true, delay: { "show": 0, "hide": 2000 }, trigger:"hover"});
    // $('[data-toggle="tooltip"]').tooltip('hide');


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
     var el = $(':focus');
     var newVal = el.val();

     switch (el.attr('id')) {
      case 'dif_id-input':
        if (dif_ids.indexOf(newVal) == -1) {
            $('#dif_id').val("");
        } else {
            $('#dif_id').val(newVal);
        }
        break;
      case 'award-input':
        if (awards.indexOf(newVal) == -1) {
            $('#award').val("");
        } else {
            $('#award').val(newVal);
        }
        break;
      }
      var selected = {
        dif_id: $('#dif_id').val(),
        award: $('#award').val(),
        all_selected: 1//$('#all_selected:checked').val() ? 1 : 0
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
    if (abstract_ind == 6) {
      $("#abstract_title").text('Project Abstract');
      $("#abstract").css({top:y-400+"px", left:x+"px"}).show();
    } else {
      $("#abstract").css({top:y-400+"px", left:x-300+"px"}).show();
    }
  });

  $('.close_abstract_btn').click(function() {
    $("#abstract").hide();
  });
  

  var map = new MapClient();
  var styles = [
    new ol.style.Style({
      stroke: new ol.style.Stroke({
        color: 'rgba(255, 0, 0, 0.8)',
        width: 2
      }),
      fill: new ol.style.Fill({
        color: 'rgba(255, 0, 0, 0.3)'
      })
    }),
    new ol.style.Style({
      image: new ol.style.Circle({
        radius: 4,
        stroke: new ol.style.Stroke({
          color: 'rgba(255, 0, 0, 0.8)',
          width: 2
        }),
        fill: new ol.style.Fill({
          color: 'rgba(255, 0, 0, 0.3)'
        })
      })
    })
  ];
  
  $('.geometry-button').click(function(event) {
    var header = $(this).closest('table').find('th');
    var dif_id_ind, geometry_ind = 999;
    for (var i in header) {
      if (header[i].tagName != "TH") continue;
      var label = header[i].innerText;
      if (label == "DIF ID") dif_id_ind = i;
      if (label == "Geometry") geometry_ind = i;
    }
    var row = $(this).closest('tr');
    var dif_id = row.find('td').eq(dif_id_ind).text();
    var geometry = row.find('td').eq(geometry_ind).text();
    $("#geometry_title").text(dif_id);
    plotGeometry(map, geometry, styles);
    var x = event.pageX;
    var y = event.pageY;
    if (geometry_ind == 7) {
      $("#geometry_title").text('Project Spatial Bounds');
      $("#geometry").css({top:y-400+"px", left:x+"px"}).show();
    } else {
      $("#geometry").css({top:y-400+"px", left:x-300+"px"}).show();
    }
  });

  $('.close_geom_btn').click(function() {
    $("#geometry").hide();
  });
  

  //Make the DIV element draggagle:
  dragElement(document.getElementById(("abstract")));
  dragElement(document.getElementById(("geometry")));

});

function MapClient() {
  proj4.defs('EPSG:3031', '+proj=stere +lat_0=-90 +lat_ts=-71 +lon_0=0 +k=1 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs');
  var projection = ol.proj.get('EPSG:3031');
  projection.setWorldExtent([-180.0000, -90.0000, 180.0000, -60.0000]);
  projection.setExtent([-8200000, -8200000, 8200000, 8200000]);
  map = new ol.Map({ // set to GMRT SP bounds
  target: 'map',
    view: new ol.View({
        center: [0,0],
        zoom: 2,
        projection: projection,
        minZoom: 1,
        maxZoom: 10
    }),  
  });

  var api_url = 'https://api.usap-dc.org:8443/wfs?';
  var gmrt = new ol.layer.Tile({
    type: 'base',
    title: "GMRT Synthesis",
    source: new ol.source.TileWMS({
        url: "https://www.gmrt.org/services/mapserver/wms_SP?request=GetCapabilities&service=WMS&version=1.3.0",
        params: {
        layers: 'South_Polar_Bathymetry'
        }
    })
  });
  map.addLayer(gmrt);

  var lima = new ol.layer.Tile({
    title: "LIMA 240m",
    visible: true,
    source: new ol.source.TileWMS({
        url: api_url,
        params: {
        layers: "LIMA 240m",
        transparent: true
        }
    })
  });
  map.addLayer(lima);

  return map;
}

function plotGeometry(map, geometry, styles) {

  //first remove previous geometries
  removeLayerByName(map, "geometry");

  var format = new ol.format.WKT();

  var feature = format.readFeature(geometry, {
    dataProjection: 'EPSG:4326',
    featureProjection: 'EPSG:3031'
  });

  var source = new ol.source.Vector({
    features: [feature]
  });

  var layer = new ol.layer.Vector({
    source: source,
    style: styles,
    title: "geometry"
  });

  map.addLayer(layer);

  var extent = map.getView().getProjection().getExtent();
  //zoom to polygon
  if (feature.getGeometry().getType() == "Polygon") {
    extent = source.getExtent();
  }
  map.getView().fit(extent, map.getSize());
}

/*
  A function to remove a layer using its name/title
*/
function removeLayerByName(map, name) {
  var layersToRemove = [];
  map.getLayers().forEach(function (layer) {
    if (layer.get('title') !== undefined && layer.get('title') === name) {
        layersToRemove.push(layer);
    }
  });

  var len = layersToRemove.length;
  for(var i = 0; i < len; i++) {
      map.removeLayer(layersToRemove[i]);
  }
}


function updateMenusWithSelected(selected, reset) {
  selected = selected || {};
  if (selected.dif_id === undefined) return;
  return $.ajax({
    method: 'GET',
    url: window.location.protocol + '//' + window.location.hostname + '/filter_dif_menus',
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

      // $('[data-toggle="tooltip"]').tooltip({container: 'body'});

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
