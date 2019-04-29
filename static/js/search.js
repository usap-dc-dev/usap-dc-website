
var persons = [];
var titles = [];
var sci_programs = [];
var nsf_programs = [];
var awards = [];
var repos = [];

$(document).ready(function() {
    $(document).ajaxStart(function () { $("html").addClass("wait"); });
    $(document).ajaxStop(function () { $("html").removeClass("wait"); });
    $('[data-toggle="popover"]').popover({html: true, delay: { "show": 0, "hide": 2000 }, trigger:"hover"});

    var search_params = JSON.parse($("#search_params").text());

    $('#dp_title').typeahead({autoSelect: false});
    $('#sci_program-input').typeahead({autoSelect: false});
    $('#person-input').typeahead({autoSelect: false});
    $('#nsf_program-input').typeahead({autoSelect: false});
    $('#award-input').typeahead({autoSelect: false});
    $('#repo-input').typeahead({autoSelect: false});
    
    $('#person, #parameter, #nsf_program, #award, #sci_program, #db_type, #repo').change(function() {
        $('[data-toggle="tooltip"]').tooltip('hide');

        var selected = {
            person: $('.selectpicker[name="person"]').val(),
            db_type: $('.selectpicker[name="db_type"]').val(),
            nsf_program: $('.selectpicker[name="nsf_program"]').val(),
            award: $('.selectpicker[name="award"]').val(),
            sci_program: $('.selectpicker[name="sci_program"]').val(),
            repo: $('.selectpicker[name="repo"]').val(),
        };
         updateMenusWithSelected(selected, false);
    });

    $('#award-input, #sci_program-input, #person-input, #nsf_program-input, #dp_type-input, #repo-input').focus(function() {
        var el = $(this);
        var newVal = el.val();
        if (newVal == "All") {
            el.val("");
        }
    });

    $('#award-input, #sci_program-input, #person-input, #nsf_program-input, #dp_type-input, #repo-input').blur(function() {
        var bluredElement = $(this);
        if (bluredElement.val() === "") {
            bluredElement.val("All");
        }
    });

    $('#award-input, #sci_program-input, #person-input, #nsf_program-input, #repo-input').change(function() {
        // need to put in a delay so that some browsers (eg Firefox) can catch up with the focus
      
        window.setTimeout(function() {
            var el = $(':focus');
            var newVal = el.val();
            switch (el.attr('id')) {
                case 'sci_program-input':
                    if (sci_programs.indexOf(newVal) == -1) {
                        $('#sci_program-input').val("All");
                        $('.selectpicker[name="sci_program"]').val("");
                    } else {
                        $('.selectpicker[name="sci_program"]').val(newVal);
                    }
                    break;
                case 'award-input':
                    if (awards.indexOf(newVal) == -1) {
                        $('#award-input').val("All");
                        $('.selectpicker[name="award"]').val("");
                    } else {
                        $('.selectpicker[name="award"]').val(newVal);
                    }
                    break;
                case 'person-input':
                    if (persons.indexOf(newVal) == -1) {
                        $('#person-input').val("All");
                        $('.selectpicker[name="person"]').val("");
                    } else {
                        $('.selectpicker[name="person"]').val(newVal);
                    }
                    break;
                case 'nsf_program-input':
                    if (nsf_programs.indexOf(newVal) == -1) {
                        $('#nsf_program-input').val("All");
                        $('.selectpicker[name="nsf_program"]').val("");
                    } else {
                        $('.selectpicker[name="nsf_program"]').val(newVal);
                    }
                    break;
                case 'repo-input':
                    if (repos.indexOf(newVal) == -1) {
                        $('#repo-input').val("All");
                        $('.selectpicker[name="repo"]').val("");
                    } else {
                        $('.selectpicker[name="repo"]').val(newVal);
                    }
                    break;
                default:
                    return;
            }

            var selected = {
                person: $('.selectpicker[name="person"]').val(),
                db_type: $('.selectpicker[name="db_type"]').val(),
                nsf_program: $('.selectpicker[name="nsf_program"]').val(),
                award: $('.selectpicker[name="award"]').val(),
                sci_program: $('.selectpicker[name="sci_program"]').val(),
                repo: $('.selectpicker[name="repo"]').val()
            };
            updateMenusWithSelected(selected, false);   
        }, 300);
    });


    if ($(location).attr('pathname') == '/dataset_search') {
        var mc = new MapClient();

        $('#data_link').submit(function() {
        var features = mc.vectorSrc.getFeatures();
        if (features.length > 0) {
            var geom = mc.vectorSrc.getFeatures()[0].getGeometry();
            var interpCoords = interpolateLineString(geom.getCoordinates()[0],10);
            var wkt = (new ol.format.WKT()).writeGeometry(new ol.geom.Polygon([interpCoords]));
            $('input[name="spatial_bounds_interpolated"]').val(wkt);
        } else {
            $('input[name="spatial_bounds_interpolated"]').val('');
        }
        });

        $('#map-modal').on('shown.bs.modal', function() {
        mc.map.updateSize();
        });
    }
  
  $('.abstract-button').click(function(event) {
    var header = $(this).closest('table').find('th');
    var abstract_ind, type_ind = 999;
    for (var i in header) {
      if (header[i].tagName != "TH") continue;
      var label = header[i].innerText;
      if (label == "Abstract") abstract_ind = i;
      if (label == "Type") type_ind = i;
    }
    var row = $(this).closest('tr');
    var abstract = row.children('td').eq(abstract_ind).text();
    $("#abstract_text").html(abstract);
    var x = event.pageX;
    var y = event.pageY;

    var type = row.children('td').eq(type_ind).text();
    $("#abstract_title").text(type +' Abstract');
    $("#abstract").css({top:y-400+"px", left:x+"px"}).show();

  });

  $('.close_abstract_btn').click(function() {
    $("#abstract").hide();
  });
  

  var map = new MapClient2();
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
    var geometry_ind, type_ind = 999;
    for (var i in header) {
      if (header[i].tagName != "TH") continue;
      var label = header[i].innerText;
      if (label == "Geometry") geometry_ind = i;
      if (label == "Type") type_ind = i;
    }
    var row = $(this).closest('tr');
    var geometry = row.children('td').eq(geometry_ind).text();
    if (geometry[0] == '[') {
        geometry = JSON.parse(geometry);
    } else {
        geometry = [geometry];
    }    
    plotGeometry(map, geometry, styles);

    var x = event.pageX;
    var y = event.pageY;

    var type = row.find('td').eq(type_ind).text();
    $("#geometry_title").text(type +' Spatial Bounds');
    $("#geometry").css({top:y-400+"px", left:x+"px"}).show();

  });

  $('.close_geom_btn').click(function() {
    $("#geometry").hide();
  });
  

  //Make the DIV element draggagle:
  dragElement(document.getElementById(("abstract")));
  dragElement(document.getElementById(("geometry")));

  updateMenusWithSelected(search_params, false);

});

function MapClient2() {
  proj4.defs('EPSG:3031', '+proj=stere +lat_0=-90 +lat_ts=-71 +lon_0=0 +k=1 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs');
  var projection = ol.proj.get('EPSG:3031');
  projection.setWorldExtent([-180.0000, -90.0000, 180.0000, -60.0000]);
  projection.setExtent([-8200000, -8200000, 8200000, 8200000]);
  var map = new ol.Map({ // set to GMRT SP bounds
  target: 'show_on_map',
    view: new ol.View({
        center: [0,0],
        zoom: 2,
        projection: projection,
        minZoom: 1,
        maxZoom: 10
    }),  
  });

  var api_url = 'http://api.usap-dc.org:81/wfs?';
  var gmrt = new ol.layer.Tile({
    type: 'base',
    title: "GMRT Synthesis",
    source: new ol.source.TileWMS({
        url: "http://gmrt.marine-geo.org/cgi-bin/mapserv?map=/public/mgg/web/gmrt.marine-geo.org/htdocs/services/map/wms_sp.map",
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

  for (let geom of geometry) {
    var feature = format.readFeature(geom, {
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


function updateMenusWithSelected(selected) {
    selected = selected || {};

    if($(location).attr('pathname') == '/dataset_search') {
        selected['dp_type'] = 'Dataset';
    } else {
        selected['dp_type'] = 'Project';
    }
    return $.ajax({
      method: 'GET',
      url: 'http://' + window.location.hostname + '/filter_joint_menus',
      data: selected,

      success: function(opts) {

        //move 'Other' to end of list
        var ind = opts.repo.indexOf('Other');
        if (ind > -1) {
          opts.repo.splice(ind, 1);
        }
        opts.repo.push('Other');

        for (var menu_name in opts) {
            // console.log('filling opts: ' + menu_name +", " + selected[menu_name]);
            fill_opts(menu_name, opts[menu_name], selected[menu_name]);
        }

        $('[data-toggle="tooltip"]').tooltip({container: 'body'});

        persons = opts.person;
        titles = opts.dp_title;
        sci_programs = opts.sci_program;
        nsf_programs = opts.nsf_program;
        awards = opts.award;
        repos = opts.repo;

        $('#dp_title').data('typeahead').source = makeAutocompleteSource(titles);
        $('#sci_program-input').data('typeahead').source = makeAutocompleteSource(sci_programs);
        $('#nsf_program-input').data('typeahead').source = makeAutocompleteSource(nsf_programs);
        $('#person-input').data('typeahead').source = makeAutocompleteSource(persons);
        $('#award-input').data('typeahead').source = makeAutocompleteSource(awards);
        $('#repo-input').data('typeahead').source = makeAutocompleteSource(repos);
      }
    });
}


function makeAutocompleteSource(wordlist) {
  return function(term, responseFn) {
      var re = new RegExp($.ui.autocomplete.escapeRegex(term),'i');
      var ret = wordlist.filter(function(t) {return re.test(t); });
      ret.unshift(term);
      return responseFn(ret);
  };
}

function resetForm() {
    document.getElementById("data_link").reset();
    $("#free_text").val("");
    $("#dp_title").val("");
    $("#spatial_bounds").text("");
    $('#clear-polygon').click();
    $("#exclude").prop('checked', false);
    $("#search_btn").click();
}

function fill_opts(menu_name, opts, selected) {
    var $select = $('.selectpicker[name='+'"'+menu_name+'"]');
    $select.selectpicker({width:'225px'});

    $select.empty();
    $select.append('<option value="">All</option>');
    for (var opt of opts) {
        $select.append('<option value="'+opt+'">'+opt+'</option>');
    }

    switch (menu_name) {
        case 'award':
            if(!selected) $('#award-input').val("All"); else $('#award-input').val(selected);
            break;
        case 'sci_program':
            if(!selected) $('#sci_program-input').val("All"); else $('#sci_program-input').val(selected);
            break;
        case 'person':
            if(!selected) $('#person-input').val("All"); else $('#person-input').val(selected);
            break;
        case 'nsf_program':
            if(!selected) $('#nsf_program-input').val("All"); else $('#nsf_program-input').val(selected);
            break;
        case 'dp_type':
            if(!selected) $('#dp_type-input').val("All"); else $('#dp_type-input').val(selected);
            break;
        case 'repo':
            if(!selected) $('#repo-input').val("All"); else $('#repo-input').val(selected);
            break;
    }

    $select.selectpicker('refresh');
    $select.selectpicker('val', selected);
    
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



function MapClient() {
    proj4.defs('EPSG:3031', '+proj=stere +lat_0=-90 +lat_ts=-71 +lon_0=0 +k=1 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs');
    var projection = ol.proj.get('EPSG:3031');
    projection.setWorldExtent([-180.0000, -90.0000, 180.0000, -60.0000]);
    projection.setExtent([-8200000, -8200000, 8200000, 8200000]);
    this.map = new ol.Map({ // set to GMRT SP bounds
    target: 'map',
    view: new ol.View({
        center: [0,0],
        zoom: 2,
        projection: projection,
        minZoom: 1,
        maxZoom: 10
    }),
    
    });

    var api_url = 'http://api.usap-dc.org:81/wfs?';
    var gmrt = new ol.layer.Tile({
    type: 'base',
    title: "GMRT Synthesis",
    source: new ol.source.TileWMS({
        url: "http://gmrt.marine-geo.org/cgi-bin/mapserv?map=/public/mgg/web/gmrt.marine-geo.org/htdocs/services/map/wms_sp.map",
        params: {
        layers: 'South_Polar_Bathymetry'
        }
    })
    });
    this.map.addLayer(gmrt);

    var lima = new ol.layer.Tile({
    // type: 'base',
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
    this.map.addLayer(lima);

    var difAeronomyAndAstrophysics = new ol.layer.Tile({
    title: "Antarctic Astrophysics and Geospace Sciences",
    source: new ol.source.TileWMS({
        url: api_url,
        params: {
        layers: 'Astro-Geo',
        transparent: true
        }
    })
    });
    this.map.addLayer(difAeronomyAndAstrophysics);
    
    var difEarthSciences = new ol.layer.Tile({
    title: "Antarctic Earth Sciences",
    source: new ol.source.TileWMS({
        url: api_url,
        params: {
        layers: 'Earth',
        transparent: true
        }
    })
    });
    this.map.addLayer(difEarthSciences);
    
    var difGlaciology = new ol.layer.Tile({
    title: "Antarctic Glaciology",
    source: new ol.source.TileWMS({
        url: api_url,
        params: {
        layers: 'Glacier',
        transparent: true
        }
    })
    });           
    this.map.addLayer(difGlaciology);
    
    var difOceanAndAtmosphericSciences = new ol.layer.Tile({
    title: "Antarctic Ocean and Atmospheric Sciences",
    source: new ol.source.TileWMS({
        url: api_url,
        params: {
        layers: 'Ocean-Atmosphere',
        transparent: true
        }
    })
    });                            
    this.map.addLayer(difOceanAndAtmosphericSciences);
    
    var difOrganismsAndEcosystems = new ol.layer.Tile({
    title: "Antarctic Organisms and Ecosystems",
    source: new ol.source.TileWMS({
        url: api_url,
        params: {
        layers: 'Bio',
        transparent: true
        }
    })
    });
    this.map.addLayer(difOrganismsAndEcosystems);
    
    var difINT = new ol.layer.Tile({
    title: "Antarctic Integrated System Science",
    source: new ol.source.TileWMS({
        url: api_url,
        params: {
        layers: 'Integrated',
        transparent: true
        }
    })
    });

    var mousePosition = new ol.control.MousePosition({
        //coordinateFormat: ol.coordinate.createStringXY(2),
        projection: 'EPSG:4326',
        target: document.getElementById('mouse-position'),
        undefinedHTML: '&nbsp;',
    coordinateFormat: function(pos) {
        var [lon,lat] = pos;
        lat = Number(lat).toFixed(2);
        lon = Number(lon).toFixed(2);
        return (lat + '&deg;, ') + (lon + '&deg; ');
    }
    });
    this.map.addControl(mousePosition);

    var popup = new ol.Overlay.Popup({"panMapIfOutOfView":false});
    this.map.addOverlay(popup);
    this.map.on('click', function(evt) {
        var tolerance = [7, 7];
        var x = evt.pixel[0];
        var y = evt.pixel[1];
        var min_px = [x - tolerance[0], y + tolerance[1]];
        var max_px = [x + tolerance[0], y - tolerance[1]];
        var min_coord = this.getCoordinateFromPixel(min_px);
        var max_coord = this.getCoordinateFromPixel(max_px);
        var bbox = min_coord[0]+','+min_coord[1]+','+max_coord[0]+','+max_coord[1];
        
        var layers = '';
        if (difAeronomyAndAstrophysics.getVisible())
            layers += 'Astro-Geo';
        if (difEarthSciences.getVisible())
            layers += ',Earth';
        if (difGlaciology.getVisible())
            layers += ',Glacier';
        if (difOceanAndAtmosphericSciences.getVisible())
            layers += ',Ocean-Atmosphere';
        if (difOrganismsAndEcosystems.getVisible())
            layers += ',Bio';
        if (difINT.getVisible())
            layers += ',Integrated';
        if (layers.charAt(0) == ',') 
            layers = layers.substring(1);
        if (layers.length > 0) {
            $.ajax({
                type: "GET",
                url: 'http://' + window.location.hostname + '/getfeatureinfo?', //"http://www.usap-data.org/usap_layers.php"
                data: {
                "query_layers" : layers,
                "layers": layers,
                "bbox" : bbox,
                "request": "GetFeatureInfo",
                "info_format" : "text/html",
                "service": "WMS",
                "version": "1.1.0",
                "width": 6,
                "height": 6,
                "X": 3,
                "Y": 3,
                "FEATURE_COUNT": 50,
                "SRS": "EPSG:3031"
                },
                success: function(msg){
                if (msg) {
                    // count how many datasets were returned
                    var count = (msg.match(/===========/g) || []).length;
                    msg = "<h6>Number of data sets: " + count + "</h6>" + msg;
                    var $msg = $('<div style="font-size:0.8em;max-height:100px;">'+msg+'</div>');
                    $msg.find('img').each(function() {
                    if (/arrow_show/.test($(this).attr('src'))) {
                        $(this).attr('src', 'http://www.marine-geo.org/imgs/arrow_show.gif');
                    } else if (/arrow_hide/.test($(this).attr('src'))) {
                        $(this).attr('src', 'http://www.marine-geo.org/imgs/arrow_hide.gif');
                    }
                    });
                    popup.show(evt.coordinate, $msg.prop('outerHTML'));
                    $('.turndown').click(function(){
                    var isrc = 'http://www.marine-geo.org/imgs/arrow_hide.gif';
                    if ($(this).find('img').attr('src')=='http://www.marine-geo.org/imgs/arrow_hide.gif')
                        isrc = 'http://www.marine-geo.org/imgs/arrow_show.gif';
                    $(this).find('img').attr('src',isrc);
                    $(this).parent().find('.tcontent').toggle();
                    //map.popups[0].updateSize();
                    });
                }
            }
        });
    }
    });

    this.vectorSrc = new ol.source.Vector();
    var vectorLayer = new ol.layer.Vector({ source: this.vectorSrc });
    this.map.addLayer(vectorLayer);
    var self = this;

    if ($('#spatial_bounds').val() !== '') {
      try {
          var geom = (new ol.format.WKT()).readGeometry($('#spatial_bounds').val());
          geom.transform('EPSG:4326', 'EPSG:3031');
          self.vectorSrc.clear();
          self.vectorSrc.addFeature(new ol.Feature(geom));
      } catch (e) {
          console.log('invalid wkt geometry');
      }
    }
    
    $('.drawing-button').on('click', function() {
      var mode = $(this).attr('data-mode');
      self.setDrawMode(mode);
      self.setDrawingTool(mode);
    });

    $('#spatial_bounds').on('change', function() {
      try {
          var geom = (new ol.format.WKT()).readGeometry($('spatial_bounds').val());
          geom.transform('EPSG:4326', 'EPSG:3031');
          self.vectorSrc.clear();
          self.vectorSrc.addFeature(new ol.Feature(geom));
      } catch (e) {
          console.log('invalid wkt geometry');
      }
    });

    $('#clear-polygon').click(function() {
      $('#spatial_bounds').val('');
      self.vectorSrc.clear();
    });

}


function interpolateGreatCircles(coords,resolution) {
    var i,j;
    var ret = [];
    for (i=0, j=1; j < coords.length; i++, j++) {
    var start = coords[i];
    var end = coords[j];
    if (start[0] == end[0] && start[1] == end[1]) {
        ret.push(start);
        ret.push(end);
    } else {
        start = {x: start[0], y: start[1]};
        end = {x: end[0], y: end[1]};
        var generator = new arc.GreatCircle(start,end);
        var line = generator.Arc(resolution);
        ret = ret.concat(line.geometries[0].coords);
    }
    }
    return ret;
}

function interpolateLineString(coords,resolution) {
    var i,j;
    var ret = [];
    for (i=0, j=1; j < coords.length; i++, j++) {
    var start = coords[i];
    var end = coords[j];
    for (var n = 0; n < resolution; n++) {
        var t = n/resolution;
        var x = (1-t)*start[0] + t*end[0];
        var y = (1-t)*start[1] + t*end[1];
        ret.push([x,y]);
    }
    }
    ret.push(coords[coords.length-1]);
    return ret;
}

MapClient.prototype.setDrawingTool = function() {
    this.map.removeInteraction(this.drawingTool);
    var value = this.getDrawMode(); 
    if (value !== 'None') {
    var maxPoints, geometryFunction;
    if (value == 'Box') {
        value = 'LineString';
        maxPoints = 2;
        geometryFunction = function(coordinates, geometry) {
        if (!geometry) {
            geometry = new ol.geom.Polygon(null);
        }
        var start = coordinates[0];
        var end = coordinates[1];
        geometry.setCoordinates([
            [start, [start[0], end[1]], end, [end[0], start[1]], start]
        ]);
        return geometry;
        };
    }
    this.drawingTool = new ol.interaction.Draw({
        source: this.vectorSrc,
        type: value,
        geometryFunction: geometryFunction,
        maxPoints: maxPoints
    });
    this.map.addInteraction(this.drawingTool);
    
    var self = this;
    this.drawingTool.on(ol.interaction.DrawEventType.DRAWSTART, function() {self.vectorSrc.clear();});

    this.drawingTool.on(ol.interaction.DrawEventType.DRAWEND, function(e) {
        var geom = e.feature.getGeometry().clone();
        geom.transform('EPSG:3031', 'EPSG:4326');
        var coords = geom.getCoordinates();
        coords = coords.map(function(ring) { return ring.map(function(xy) { return [Number(xy[0]).toFixed(3), Number(xy[1]).toFixed(3)]; }); });
        geom.setCoordinates(coords);
        $('[name="spatial_bounds"]').val((new ol.format.WKT()).writeGeometry(geom));
    });
    }
};

MapClient.prototype.getDrawMode = function() {
    return $('.drawing-button.draw-active').attr('data-mode');
};

MapClient.prototype.setDrawMode = function(str) {
    $('#drawing-buttons .drawing-button').removeClass('draw-active');
    $('#drawing-buttons .drawing-button[data-mode="' + str + '"]').addClass('draw-active');
};

