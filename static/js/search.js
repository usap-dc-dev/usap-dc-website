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
    	case 'project':
    		if(!selected) $('#project-input').val("All"); else $('#project-input').val(selected);
    		break;
    	case 'person':
    		if(!selected) $('#person-input').val("All"); else $('#person-input').val(selected);
    		break;
    	case 'program':
    		if(!selected) $('#program-input').val("All"); else $('#program-input').val(selected);
    		break;
    }

    $select.selectpicker('refresh');
    $select.selectpicker('val', selected);
    
}


function updateMenusWithSelected(selected, reset) {
    selected = selected || {};
    return $.ajax({
	method: 'GET',
	url: 'http://' + window.location.hostname + '/filter_search_menus',
	data: selected,

	success: function(opts) {
		if (reset) {
	    	document.getElementById("data_link").reset();
	    }

	    for (var menu_name in opts) {
	    	// console.log('filling opts: ' + menu_name +", " + selected[menu_name]);
			fill_opts(menu_name, opts[menu_name], selected[menu_name]);
	    }

	    $('[data-toggle="tooltip"]').tooltip({container: 'body'});

		}
    });
}

function resetForm() {
	updateMenusWithSelected({}, true);
}

var mc;
$(document).ready(function() {
    $(document).ajaxStart(function () { $("html").addClass("wait"); });
    $(document).ajaxStop(function () { $("html").removeClass("wait"); });
	$('[data-toggle="popover"]').popover({html: true, delay: { "show": 0, "hide": 2000 }, trigger:"hover"});


    var check = $("#entire_region");
    var w = $('input[name="west"]');
    var e = $('input[name="east"]');
    var s = $('input[name="south"]');
    var n = $('input[name="north"]');
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

    
    $('#person, #parameter, #program, #award, #project').change(function(e) {

		$('[data-toggle="tooltip"]').tooltip('hide');

		var selected = {
		    person: $('.selectpicker[name="person"]').val(),
		    parameter: $('.selectpicker[name="parameter"]').val(),
		    program: $('.selectpicker[name="program"]').val(),
		    award: $('.selectpicker[name="award"]').val(),
		    project: $('.selectpicker[name="project"]').val(),
		};
		 updateMenusWithSelected(selected, false);
    });

    $('#award-input, #project-input, #person-input, #program-input').focus(function() {
    	var el = $(this);
		var newVal = el.val();
		if (newVal == "All") {
			el.val("");
		}
	});

    $('#award-input, #project-input, #person-input, #program-input').blur(function() {
		var bluredElement = $(this);
		if (bluredElement.val() === "") {
			bluredElement.val("All");
		}
	});

    $('#award-input, #project-input, #person-input, #program-input').change(function() {
		// need to put in a delay so that some broswers (eg Firefox) can catch up wit the focus
		window.setTimeout(function() {
			var el = $(':focus');
			var newVal = el.val();
			switch (el.attr('id')) {
				case 'project-input':
					if (projects.indexOf(newVal) == -1) {
						$('#project-input').val("All");
						$('.selectpicker[name="project"]').val("");
					} else {
						$('.selectpicker[name="project"]').val(newVal);
					}
					break;
				case 'award-input':
					if (awards_str.indexOf(newVal) == -1) {
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
				case 'program-input':
					if (programs.indexOf(newVal) == -1) {
						$('#program-input').val("All");
						$('.selectpicker[name="program"]').val("");
					} else {
						$('.selectpicker[name="program"]').val(newVal);
					}
					break;
				default:
					return;
			}

			var selected = {
			    person: $('.selectpicker[name="person"]').val(),
			    parameter: $('.selectpicker[name="parameter"]').val(),
			    program: $('.selectpicker[name="program"]').val(),
			    award: $('.selectpicker[name="award"]').val(),
			    project: $('.selectpicker[name="project"]').val(),
			};
			updateMenusWithSelected(selected, false);	
		}, 300);
	});



    mc = new MapClient();

    //$('#spatial-tabs').tabs();

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

    /*$('[name="title"]').autocomplete({source: 'http://www.usap-dc.org/titles'});
      $('[name="parameter"]').autocomplete({source: 'http://www.usap-dc.org/parameter_search'});*/

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
 //    $('[name="parameter"]').typeahead({
	// source: makeAutocompleteSource(projects),
	// autoSelect: false
 //    });


    $('#project-input').typeahead({
	source: makeAutocompleteSource(projects),
	autoSelect: false
    });
    $('#person-input').typeahead({
	source: makeAutocompleteSource(persons),
	autoSelect: false
    });
    $('#program-input').typeahead({
	source: makeAutocompleteSource(programs),
	autoSelect: false
    });
    $('#award-input').typeahead({
	source: makeAutocompleteSource(awards_str),
	autoSelect: false
    });

    $('#datepicker').datepicker({
	format: "yyyy-mm-dd",
	startView: 2
    });

    $('#map-modal').on('shown.bs.modal', function() {
	mc.map.updateSize();
    });

    $('.selectpicker').selectpicker({
	title: 'All',
	width: 'fit'
    });

    updateMenusWithSelected(search_params, false);
    
    //$('.dropdown').each(function(i,elem) { $(elem).makeDropdownIntoSelect('','All'); });
});




function MapClient() {
    proj4.defs('EPSG:3031', '+proj=stere +lat_0=-90 +lat_ts=-71 +lon_0=0 +k=1 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs');
    var projection = ol.proj.get('EPSG:3031');
    projection.setWorldExtent([-180.0000, -90.0000, 180.0000, -60.0000]);
    projection.setExtent([-8200000, -8200000, 8200000, 8200000]);
    this.map = new ol.Map({	// set to GMRT SP bounds
	target: 'map',
	view: new ol.View({
	    center: [0,0],
	    zoom: 1,
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


//    var zoomcontrol = new ol.control.Zoom();
    //    map.addControl(zoomcontrol);

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

    this.vectorSrc = new ol.source.Vector();
    var vectorLayer = new ol.layer.Vector({ source: this.vectorSrc });
    this.map.addLayer(vectorLayer);
    var self = this;
    $('.drawing-button').on('click', function(e) {
	var mode = $(this).attr('data-mode');
	self.setDrawMode(mode);
	self.setDrawingTool(mode);
    });

    $('[name="spatial_bounds"]').on('change', function() {
	try {
	    var geom = (new ol.format.WKT()).readGeometry($('[name="spatial_bounds"]').val());
	    geom.transform('EPSG:4326', 'EPSG:3031');
	    self.vectorSrc.clear();
	    self.vectorSrc.addFeature(new ol.Feature(geom));
	} catch (e) {
	    console.log('invalid wkt geometry');
	}
    });

    $('#clear-polygon').click(function() {
	$('[name="spatial_bounds"]').val('');
	self.vectorSrc.clear();
    });

    //$('#close-modal')
    
    /*
    var styles = [
        
        new ol.style.Style({
            stroke: new ol.style.Stroke({
		color: 'blue',
		width: 3
            }),
            fill: new ol.style.Fill({
		color: 'rgba(0, 0, 255, 0.1)'
            })
        }),
        new ol.style.Style({
            image: new ol.style.Circle({
		radius: 5,
		fill: new ol.style.Fill({
		    color: 'black'
		})
            }),
            geometry: function(feature) {
		// return the coordinates of the first ring of the polygon
		var coordinates = feature.getGeometry().getCoordinates()[0];
		return new ol.geom.MultiPoint(coordinates);
            }
        })
    ];
        
    this.bboxSrc = new ol.source.Vector();
    var bboxLayer = new ol.layer.Vector({ source: this.bboxSrc, style: styles });
    this.map.addLayer(bboxLayer);

    $.ajax({
	method: 'GET',
	url: 'http://www.usap-dc.org/geometries',
	success: function(msg) {
	    var featureCollection = {
		type: 'FeatureCollection',
		crs: {
		    type: 'name',
		    properties: {
			name: 'EPSG:3031'
		    }
		},
	    };
	    var features = []
	    for (var ft of msg) {
		var outer = { type: 'Feature', geometry: $.parseJSON(ft), properties: {}};
		features.push(outer);
	    }
	    featureCollection.features = features;
	    self.bboxSrc.addFeatures((new ol.format.GeoJSON()).readFeatures(JSON.stringify(featureCollection)));
	}
	});

    self.bboxSrc.addFeatures((new ol.format.GeoJSON()).readFeatures(JSON.stringify(geojsonObject));
     
    var feature = new ol.Feature(
	new ol.geom.Polygon([[[0, 0],[0,8200000],[8200000,8200000],[8200000,0],[0,0]]])
    );
    //feature.setStyle(circle);

    var geojsonObject = {
        'type': 'FeatureCollection',
        'crs': {
          'type': 'name',
          'properties': {
            'name': 'EPSG:3857'
          }
        },
        'features': [{
          'type': 'Feature',
          'geometry': {
            'type': 'Polygon',
            'coordinates': [[[-5e6, 6e6], [-5e6, 8e6], [-3e6, 8e6],
                [-3e6, 6e6], [-5e6, 6e6]]]
          }
        }, {
          'type': 'Feature',
          'geometry': {
            'type': 'Polygon',
            'coordinates': [[[-2e6, 6e6], [-2e6, 8e6], [0, 8e6],
                [0, 6e6], [-2e6, 6e6]]]
          }
        }, {
          'type': 'Feature',
          'geometry': {
            'type': 'Polygon',
            'coordinates': [[[1e6, 6e6], [1e6, 8e6], [3e6, 8e6],
                [3e6, 6e6], [1e6, 6e6]]]
          }
        }, {
          'type': 'Feature',
          'geometry': {
            'type': 'Polygon',
            'coordinates': [[[-2e6, -1e6], [-1e6, 1e6],
                [0, -1e6], [-2e6, -1e6]]]
          }
        }]
      };
    
    //this.bboxSrc.addFeature(feature);

    */
}

function interpolateGreatCircles(coords,resolution) {
    var i,j;
    var ret = []
    for (var i=0, j=1; j < coords.length; i++, j++) {
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
    for (var i=0, j=1; j < coords.length; i++, j++) {
	var start = coords[i];
	var end = coords[j];
	for (var n = 0; n < resolution; n++) {
	    var t = n/resolution;
	    var x = (1-t)*start[0] + t*end[0];
	    var y = (1-t)*start[1] + t*end[1];
	    ret.push([x,y])
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
	/*if (value == 'Polygon') {
	    geometryFunction = function(coordinates, geometry) {
		if (!geometry) {
		    geometry = new ol.geom.Polygon(null);
		}
		var coords = new ol.geom.LineString(coordinates[0]);
		coords.transform('EPSG:3031', 'EPSG:4326');
		coords = coords.getCoordinates();
		coords.push(coords[0]);
		coords = interpolateGreatCircles(coords, 10);
		
		// Ensure that the polygon closes exactly
		coords.pop();
		coords.push(coords[0]);
		geometry.setCoordinates([coords]);
		geometry.transform('EPSG:4326', 'EPSG:3031');
		return geometry;
		//return new ol.geom.Polygon([[[0,0],[10000,10000],[10000,0],[0,0]]]);
	    }
	    }
	if (value == 'Polygon') {
	    value = 'LineString';
	    geometryFunction = function(coordinates, geometry) {
		if (!geometry) {
		    geometry = new ol.geom.Polygon(null);
		}
		var coords = new ol.geom.LineString(coordinates);
		coords.transform('EPSG:3031', 'EPSG:4326');
		coords = coords.getCoordinates();
		coords.push(coords[0]);
		coords = interpolateGreatCircles(coords, 10);
		
		// Ensure that the polygon closes exactly
		coords.pop();
		coords.push(coords[0]);
		geometry.setCoordinates([coords]);
		geometry.transform('EPSG:4326', 'EPSG:3031');
		return geometry;
	    }
	} else*/ if (value == 'Box') {
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
	this.drawingTool.on(ol.interaction.DrawEventType.DRAWSTART, function() {self.vectorSrc.clear()});

	this.drawingTool.on(ol.interaction.DrawEventType.DRAWEND, function(e) {
	    var geom = e.feature.getGeometry().clone();
	    geom.transform('EPSG:3031', 'EPSG:4326');
	    var coords = geom.getCoordinates();
	    coords = coords.map(function(ring) { return ring.map(function(xy) { return [Number(xy[0]).toFixed(3), Number(xy[1]).toFixed(3)]; }); });
	    console.log(coords);
	    geom.setCoordinates(coords);
	    $('[name="spatial_bounds"]').val((new ol.format.WKT()).writeGeometry(geom));
	});
    }
}

MapClient.prototype.getDrawMode = function() {
    return $('.drawing-button.draw-active').attr('data-mode');
}

MapClient.prototype.setDrawMode = function(str) {
    $('#drawing-buttons .drawing-button').removeClass('draw-active');
    $('#drawing-buttons .drawing-button[data-mode="' + str + '"]').addClass('draw-active');
}
