
var persons = [];
var titles = [];
var sci_programs = [];
var nsf_programs = [];
var awards = [];
var repos = [];
// var locations = [];
var map;
var search_table_dt;
var date_col, vis_col, sel_col;

$(document).ready(function() {
    $(document).ajaxStart(function () { $("html").addClass("wait"); });
    $(document).ajaxStop(function () { $("html").removeClass("wait"); });
    $('[data-toggle="popover"]').popover({html: true, delay: { "show": 0, "hide": 2000 }, trigger:"hover"});

    // set up search table
    var header = $('#search_table').find('th');
    for (var i in header) {
      if (header[i].tagName != "TH") continue;
      var label = header[i].innerText;
      if (label == "Date Created") date_col = i;
      else if (label == "Selected") sel_col = i;
      else if (label == "Visible") vis_col = i;
    }
    search_table_dt = $('#search_table').DataTable( {
        "searching": false,
        "paging": false,
        "order": [[date_col, "desc"]]
    });

    var search_params = JSON.parse($("#search_params").text());
    search_results = JSON.parse($("#search_results").text());

    $('#dp_title').typeahead({autoSelect: false});
    $('#sci_program-input').typeahead({autoSelect: false});
    $('#person-input').typeahead({autoSelect: false});
    $('#nsf_program-input').typeahead({autoSelect: false});
    $('#award-input').typeahead({autoSelect: false});
    $('#repo-input').typeahead({autoSelect: false});
    // $('#location-input').typeahead({autoSelect: false});

    $('#spatial_bounds_interpolated').val(search_params.spatial_bounds_interpolated);
    
    $('#person, #parameter, #nsf_program, #award, #sci_program, #db_type, #repo, #location').change(function() {
        $('[data-toggle="tooltip"]').tooltip('hide');

        var selected = {
            person: $('.selectpicker[name="person"]').val(),
            db_type: $('.selectpicker[name="db_type"]').val(),
            nsf_program: $('.selectpicker[name="nsf_program"]').val(),
            award: $('.selectpicker[name="award"]').val(),
            sci_program: $('.selectpicker[name="sci_program"]').val(),
            repo: $('.selectpicker[name="repo"]').val(),
            // location: $('.selectpicker[name="location"]').val(),
        };
         updateMenusWithSelected(selected, false);
    });

    $('#award-input, #sci_program-input, #person-input, #nsf_program-input, #dp_type-input, #repo-input, #location-input').focus(function() {
        var el = $(this);
        var newVal = el.val();
        if (newVal == "All") {
            el.val("");
        }
    });

    $('#award-input, #sci_program-input, #person-input, #nsf_program-input, #dp_type-input, #repo-input, #location-input').blur(function() {
        var bluredElement = $(this);
        if (bluredElement.val() === "") {
            bluredElement.val("All");
        }
    });

    $('#award-input, #sci_program-input, #person-input, #nsf_program-input, #repo-input, #location-input').change(function() {
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
                // case 'location-input':
                //     if (locations.indexOf(newVal) == -1) {
                //         $('#location-input').val("All");
                //         $('.selectpicker[name="location"]').val("");
                //     } else {
                //         $('.selectpicker[name="location"]').val(newVal);
                //     }
                //     break;
                default:
                    return;
            }

            var selected = {
                person: $('.selectpicker[name="person"]').val(),
                db_type: $('.selectpicker[name="db_type"]').val(),
                nsf_program: $('.selectpicker[name="nsf_program"]').val(),
                award: $('.selectpicker[name="award"]').val(),
                sci_program: $('.selectpicker[name="sci_program"]').val(),
                repo: $('.selectpicker[name="repo"]').val(),
                // location: $('.selectpicker[name="location"]').val()
            };
            updateMenusWithSelected(selected, false);   
        }, 300);
    });
    results_map = null;
    
    $('#data_link').on('submit', (function(){
        getSpatialBoundsInterpolated();
    }));

    function getSpatialBoundsInterpolated() {
        var polygon_layer = getLayerByName(results_map, 'Polygon');
        var features = polygon_layer.getSource().getFeatures();
        if (features.length > 0) {
            var geom = features[0].getGeometry();
            var interpCoords = interpolateLineString(geom.getCoordinates()[0],10);
            var wkt = (new ol.format.WKT()).writeGeometry(new ol.geom.Polygon([interpCoords]));
            $('#spatial_bounds_interpolated').val(wkt);
        } else {
            $('#spatial_bounds_interpolated').val('');
        }
    }

  $('.close_abstract_btn').on('click', (function() {
    $("#abstract").hide();
  }));

  popup = new ol.Overlay.Popup({"panMapIfOutOfView":true});
  map = new MapClient2();

  $('#geom_btn').on('click', (function() {
    $("#results_map_div").toggle();
    $(this).text(function(i, text) { 
        if (results_map) {
            text === "View results on map" ? highlightVisibleRows(results_map, true) : unsetVisibleRows();
        }    
        return text === "View results on map" ? "Hide map" : "View results on map";
    })
    if (!results_map) {
        results_map = new MapClient('results_map');
        //results_map move event handler
        results_map.on('moveend', moveResultsMap);
        //results_map click event handler
        results_map.on('click', clickResultsMap);
           
        showResultsOnMap();
    }
  }));


  $('.close_geom_btn').on('click', (function() {
    $("#geometry").hide();
  }));
  



  
  //Make the DIV element draggagle:
  dragElement(document.getElementById(("abstract")));
  dragElement(document.getElementById(("geometry")));

  updateMenusWithSelected(search_params, false);

});

function showAbstract(el) {
    var header = $(el).closest('table').find('th');
    var abstract_ind, type_ind = 999;
    for (var i in header) {
      if (header[i].tagName != "TH") continue;
      var label = header[i].innerText;
      if (label == "Abstract") abstract_ind = i;
      if (label == "Type") type_ind = i;
    }
    var row = $(el).closest('tr');
    var abstract = row.children('td').eq(abstract_ind).text();
    $("#abstract_text").html(abstract);
    var x = event.pageX;
    var y = event.pageY;

    var type = row.children('td').eq(type_ind).text();
    $("#abstract_title").text(type +' Abstract');
    $("#abstract").css({top:y-400+"px", left:x+"px"}).show();   
  }


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
      radius: 5,
      stroke: new ol.style.Stroke({
        color: 'rgba(0, 0, 0, 1)',
        width: 2
      }),
      fill: new ol.style.Fill({
        color: 'rgba(255, 255, 0, 1)'
      })
    })
  })
];


var results_styles = [
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
          color: 'rgba(0, 0, 0, 1)',
          width: 2
        }),
        fill: new ol.style.Fill({
          color: 'rgba(255, 0, 0, 1)'
        })
      })
    })
  ];

function showOnMap(el) {
    var header = $(el).closest('table').find('th');
    var bounds_ind, geometry_ind, type_ind = 999;
    for (var i in header) {
        if (header[i].tagName != "TH") continue;
        var label = header[i].innerText;
        if (label == "Bounds Geometry") bounds_ind = i;
        if (label == "Type") type_ind = i;
        if (label == "Geometry") geometry_ind = i;
    }
    var row = $(el).closest('tr');
    var geometry = row.children('td').eq(bounds_ind).text();
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

    // show on results map
    removeLayerByName(results_map, 'Selected');
    unsetSelectedRows();
    popup.hide();

    var centroid = row.children('td').eq(geometry_ind).text();
    if (centroid[0] == '[') {
        centroid = JSON.parse(centroid);
    } else {
        centroid = [centroid];
    }   
    setSelectedSymbol(results_map, centroid, geometry);
    // highlight row in table
    row.removeClass('visible-row').addClass('selected-row'); 
}

function showResultsOnMap() {
    var results = [];
    for (var res of search_results) {
        if (res.geometry) {
            let geom = {geometry: res.geometry, properties: {uid: res.uid, title: res.title, persons: res.persons, bounds_geometry: res.bounds_geometry, centroid_geometry:res.geometry}};
            results.push(geom);
        }
    }
    plotResults(results_map, results, results_styles, 'Results', true);
}

function showBoundaries() {
    if($('#boundaries_cb').is(':checked')){
        var geometry = [];
        for (var res of search_results) {
            if (res.bounds_geometry) geometry.push(res.bounds_geometry);
        }

        plotResults(results_map, geometry, results_styles, 'Boundaries', false);
        $("#results_map").show();
    } else {
        removeLayerByName(results_map, 'Boundaries');
    }
}


function MapClient2(target='show_on_map') {
    proj4.defs('EPSG:3031', '+proj=stere +lat_0=-90 +lat_ts=-71 +lon_0=0 +k=1 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs');
    var projection = ol.proj.get('EPSG:3031');
    projection.setWorldExtent([-180.0000, -90.0000, 180.0000, -60.0000]);
    projection.setExtent([-8200000, -8200000, 8200000, 8200000]);
    var map = new ol.Map({ // set to GMRT SP bounds
        target: target,
        interactions: ol.interaction.defaults({mouseWheelZoom:false}),
        view: new ol.View({
            center: [0,0],
            zoom: 2.1,
            projection: projection,
            minZoom: 1,
            maxZoom: 10
        }),  
    });

    var api_url = 'https://api.usap-dc.org:8443/wfs?';
    var gmrt = new ol.layer.Tile({
        // type: 'base',
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

    map.addOverlay(popup);

    var layerSwitcher = new ol.control.LayerSwitcher({
        tipLabel: 'Légende'
    });
    map.addControl(layerSwitcher);

    var mousePosition = new ol.control.MousePosition({
        coordinateFormat: ol.coordinate.createStringXY(2),
        projection: 'EPSG:4326',
        target: document.getElementById('mouseposition'),
        undefinedHTML: '&nbsp;'
    });
    map.addControl(mousePosition);

    return map;
}

// change highlighted rows when map is moved or zoomed
var moveResultsMap = function() {
    $('html').addClass('wait');
    // need timeout so that wait cursor is displayed
    setTimeout(function() {
        if ($('#results_map_div').is(":visible")) {
            highlightVisibleRows(results_map, true);
        }
        $('html').removeClass('wait');
    }, 1);
}

// select a feature on the map by clicking on it
var clickResultsMap = function(evt) {
    // check we are not drawing
    if ($('#rect-icon').hasClass('draw-active') || $('#polygon-icon').hasClass('draw-active')) return;

    $('html').addClass('wait');
    // need timeout so that wait cursor is displayed
    setTimeout(function() {
        unsetSelectedRows();
        removeLayerByName(results_map, 'Selected');
        popup.hide();
        var features = [];

        if (evt.pixel) {
            results_map.forEachFeatureAtPixel(evt.pixel, 
                function(feature, layer) {
                    if (layer && layer.getProperties() && layer.getProperties().title === 'Results'){
                        features.push(feature);
                    }   
                }, {"hitTolerance":1});
        }
  
        if (features.length > 0) {
            var msg = "<h6>Number of data sets: " + features.length + "</h6>";

            for (let feature of features) {
                var props = feature.getProperties();

                // landing page
                let lp_url = '';
                if (props.uid.charAt(0) == 'p'){
                    lp_url = Flask.url_for('project_landing_page', {project_id:props.uid});
                }
                else {
                    lp_url = Flask.url_for('landing_page', {dataset_id:props.uid});
                }

                // highlight bounds geometry on map
                setSelectedSymbol(results_map, props.centroid_geometry, props.bounds_geometry);

                // select row in table
                $('#row_'+props.uid).removeClass('visible-row').addClass('selected-row'); 

                // make pop up
                msg += '<p style="font-size:0.8em;max-height:500px;"><b>Title:</b> ' + props.title;
                msg += '<br><b>Creator:</b> ' + props.persons;
                msg += '<br><a href="' + lp_url + '"><b>More info</b></a>';
                msg += '<br>===========<br> </p>';
            }
            var $msg = $('<div>' + msg + '</div>');
            popup.show(evt.coordinate, $msg.prop('innerHTML'));
    
            // set the value of the selected column to 'true'.  Faster to do it this way than at the same time as
            // setting the class above.
            for (var row of search_table_dt.rows('.selected-row')[0]) {
                search_table_dt.cell(row,sel_col).data('true');
            }        
        } 

        // reorder the table with selected rows at the top, then visible rows
        reorderTable();

        $('html').removeClass('wait');
    }, 1);
}

function getLayerByName(map, name) {
    var layer;
    map.getLayers().forEach(function(lyr) {
        if (lyr.getProperties().title == name) {
            layer = lyr;
        }
    });
    return layer;
}

function highlightVisibleRows(map, reorder) {
    // first unset the current visible rows, but don't reorder the table
    unsetVisibleRows(false);

    var extent = map.getView().calculateExtent(map.getSize());
    var rl = getLayerByName(map, 'Results');
    if (rl) {
        // set class of row to visible-row, unless it has already been selected
        rl.getSource().forEachFeatureInExtent(extent, function(feature){
            var uid = feature.getProperties().uid;
            if (!$('#row_'+uid).hasClass('selected-row')){
                $('#row_'+uid).addClass('visible-row');
            }
        }); 

        // set the value of the visible column to 'true'.  Faster to do it this way than at the same time as
        // setting the class above.
        for (var row of search_table_dt.rows('.visible-row')[0]) {
            search_table_dt.cell(row,vis_col).data('true');
        }
        // reorder the table with selected rows at the top, then visible rows
        if (reorder) reorderTable();
    }
}

function unsetVisibleRows(reorder=true){
    for (var row of search_table_dt.rows('.visible-row')[0]) {
        search_table_dt.cell(row,vis_col).data('false');
    }
    $('tr').removeClass('visible-row'); 
    // reorder the table by date
    if (reorder) reorderTable();
}

function unsetSelectedRows(){
    for (var row of search_table_dt.rows('.selected-row')[0]) {
        $(search_table_dt.row(row).node()).removeClass('selected-row').addClass('visible-row');
        search_table_dt.cell(row,sel_col).data('false');
    }
}

function reorderTable(){
    //re-order the table by selected, visible, date
    search_table_dt.order([sel_col,'desc'], [vis_col,'desc'], [date_col,'desc']).draw();
}

// Add a feature to highlight selected Spot
function setSelectedSymbol(map, centroid, bounds) {
    if (!map) return;

    if (!Array.isArray(bounds)) {
        bounds = [bounds];
    }
    if (!Array.isArray(centroid)) {
        centroid = [centroid];
    }

    geom = centroid.concat(bounds);

    var format = new ol.format.WKT();
    var selected = [];
    for (let g of geom) {  
        var feature = format.readFeature(g, {
            dataProjection: 'EPSG:4326',
            featureProjection: 'EPSG:3031'
        });
        var geometry = feature.getGeometry();
        selected.push(new ol.Feature({ geometry: geometry }));
    }

    var selectedHighlightSource = new ol.source.Vector({
        features: selected
    });

    selectedHighlightLayer = new ol.layer.Vector({
        title: 'Selected',
        source: selectedHighlightSource,
        style: styles
    });
    map.addLayer(selectedHighlightLayer);
}

function plotGeometry(map, geometry, styles) {
  //first remove previous geometries
  removeLayerByName(map, "Geometry");

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
        title: "Geometry"
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

function plotResults(this_map, results, styles, layer_name, remove_layers) {
    //first remove previous geometries
    if (remove_layers) {
        removeLayerByName(this_map, 'Boundaries');
        removeLayerByName(this_map, 'Results');
    }
  
    var format = new ol.format.WKT();
    var features = []

    for (let result of results) {
        let properties = {};
        if (result.geometry && result.properties) {
            properties = result.properties;
            geom = result.geometry;
        } else {
            geom = result;
        }
        
        if (!Array.isArray(geom)) {
            geom = [geom]
        }

        for (let g of geom) {
            // prevent NaNs
            g = g.replaceAll('180 90', '180.0 89.999');
    
            var feature = format.readFeature(g, {
                dataProjection: 'EPSG:4326',
                featureProjection: 'EPSG:3031'
            });

            feature.setProperties(properties);
            features.push(feature);
        }
    }

    var source = new ol.source.Vector({
        features: features
    });

    var layer = new ol.layer.Vector({
        source: source,
        style: styles,
        title: layer_name
    });
    this_map.addLayer(layer);
  }


/*
  A function to remove a layer using its name/title
*/
function removeLayerByName(this_map, name) {
    if (!this_map) return;
    var layersToRemove = [];
    this_map.getLayers().forEach(function (layer) {
        if (layer.get('title') !== undefined && layer.get('title') === name) {
            layersToRemove.push(layer);
        }
    });

    var len = layersToRemove.length;
    for(var i = 0; i < len; i++) {
        this_map.removeLayer(layersToRemove[i]);
    }
}


function updateMenusWithSelected(selected) {
    selected = selected || {};

    if($(location).attr('pathname') == '/dataset_search') {
        selected.dp_type = 'Dataset';
    } else {
        selected.dp_type = 'Project';
    }
    return $.ajax({
      method: 'GET',
      url: window.location.protocol + '//' + window.location.hostname + '/filter_joint_menus',
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
        // locations = opts.location;

        $('#dp_title').data('typeahead').source = makeAutocompleteSource(titles);
        $('#sci_program-input').data('typeahead').source = makeAutocompleteSource(sci_programs);
        $('#nsf_program-input').data('typeahead').source = makeAutocompleteSource(nsf_programs);
        $('#person-input').data('typeahead').source = makeAutocompleteSource(persons);
        $('#award-input').data('typeahead').source = makeAutocompleteSource(awards);
        $('#repo-input').data('typeahead').source = makeAutocompleteSource(repos);
        // $('#location-input').data('typeahead').source = makeAutocompleteSource(locations);
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
    $("#spatial_bounds_interpolated").val("");
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
        case 'location':
            if(!selected) $('#location-input').val("All"); else $('#location-input').val(selected);
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



function MapClient(target='map') {

    proj4.defs('EPSG:3031', '+proj=stere +lat_0=-90 +lat_ts=-71 +lon_0=0 +k=1 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs');
    var projection = ol.proj.get('EPSG:3031');
    projection.setWorldExtent([-180.0000, -90.0000, 180.0000, -60.0000]);
    projection.setExtent([-8200000, -8200000, 8200000, 8200000]);
    this.map = new ol.Map({ // set to GMRT SP bounds
    target: target,
    interactions: ol.interaction.defaults({mouseWheelZoom:false}),
    view: new ol.View({
        center: [0,0],
        zoom: 2.1,
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

    var mousePosition = new ol.control.MousePosition({
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

    var layerSwitcher = new ol.control.LayerSwitcher({
        tipLabel: 'Légende'
    });
    this.map.addControl(layerSwitcher);

    this.map.addOverlay(popup);

    this.vectorSrc = new ol.source.Vector();
    var vectorLayer = new ol.layer.Vector({ source: this.vectorSrc, title: 'Polygon' });
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
          var geom = (new ol.format.WKT()).readGeometry($('#spatial_bounds').val());
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

    return this.map;
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
        $('#spatial_bounds').val((new ol.format.WKT()).writeGeometry(geom));
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

