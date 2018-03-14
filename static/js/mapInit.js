function MapClient(zoom) {
	if (!zoom) {zoom = 3;}
	proj4.defs('EPSG:3031', '+proj=stere +lat_0=-90 +lat_ts=-71 +lon_0=0 +k=1 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs');
    var projection = ol.proj.get('EPSG:3031');
    projection.setWorldExtent([-180.0000, -90.0000, 180.0000, -60.0000]);
    projection.setExtent([-8200000, -8200000, 8200000, 8200000]);
    var map = new ol.Map({	// set to GMRT SP bounds
	target: 'map',
	interactions: ol.interaction.defaults({mouseWheelZoom:false}),
	view: new ol.View({
	    center: [0,0],
	    zoom: zoom,
	    projection: projection,
	    minZoom: 1,
	    maxZoom: 10
	}),
	
    });

    var api_url = 'http://api.usap-dc.org:81/wfs?';

    var scar = new ol.layer.Tile({
	type: 'base',
	title: "SCAR Coastlines",
	source: new ol.source.TileWMS({
	    url: "http://nsidc.org/cgi-bin/mapserv?",
	    params: {
		map: '/WEB/INTERNET/MMS/atlas/epsg3031_grids.map',
		layers: 'land',
		srs: 'epsg:3031',
		bgcolor: '0x00ffff',
		format: 'image/jpeg'
	    }
	})
    });
    map.addLayer(scar);
    
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

    var gmrtmask = new ol.layer.Tile({
	type: 'base',
	visible: false,
	title: "GMRT Synthesis-Mask",
	source: new ol.source.TileWMS({
	    url: "http://gmrt.marine-geo.org/cgi-bin/mapserv?map=/public/mgg/web/gmrt.marine-geo.org/htdocs/services/map/wms_sp_mask.map",
	    params: {
		layers: 'South_Polar_Bathymetry'
	    }
	})
    });
    map.addLayer(gmrtmask);


    var gma_modis = new ol.layer.Tile({
	// type: 'base',
	title: "MODIS Mosaic",
	visible: false,
	source: new ol.source.TileWMS({
	    url: "http://nsidc.org/cgi-bin/atlas_south?",
	    params: {
		layers: 'antarctica_satellite_image',
		format:'image/png',
		srs: 'epsg:3031',
		transparent: true
	    }
	})
    });
    map.addLayer(gma_modis);

 //    var modis = new ol.layer.Tile({
	// // type: 'base',
	// title: "MODIS Mosaic",
	// visible: false,
	// source: new ol.source.TileWMS({
	//     url: api_url,
	//     params: {
	// 	layers: 'MODIS',
	// 	transparent: true
	//     }
	// })
 //    });
 //    map.addLayer(modis);
 

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
    map.addLayer(lima);


    var tracks = new ol.layer.Tile({
	title: "USAP R/V Cruises",
	visible: false,
	source: new ol.source.TileWMS({
	    url: "http://www.marine-geo.org/services/mapserver/wmscontrolpointsSP?",
	    params: {
		layers: 'Tracks-Lines',
		transparent: true
	    }
	})
    });
    map.addLayer(tracks);
    
    var difINT = new ol.layer.Tile({
	title: "Integrated System Science",
	source: new ol.source.TileWMS({
	    url: api_url,
	    params: {
		layers: 'Integrated',
		transparent: true
	    }
	})
    });
    map.addLayer(difINT);
	
    var difEarthSciences = new ol.layer.Tile({
	title: "Earth Sciences",
	source: new ol.source.TileWMS({
	    url: api_url,
	    params: {
		layers: 'Earth',
		transparent: true
	    }
	})
    });
    map.addLayer(difEarthSciences);
    
    var difAeronomyAndAstrophysics = new ol.layer.Tile({
	title: "Astrophysics and Geospace Sciences",
	source: new ol.source.TileWMS({
	    url: api_url,
	    params: {
		layers: 'Astro-Geo',
		transparent: true
	    }
	})
    });
    map.addLayer(difAeronomyAndAstrophysics);
    
    var difGlaciology = new ol.layer.Tile({
	title: "Glaciology",
	source: new ol.source.TileWMS({
	    url: api_url,
	    params: {
		layers: 'Glacier',
		transparent: true
	    }
	})
    });			  
    map.addLayer(difGlaciology);
    
    var difOceanAndAtmosphericSciences = new ol.layer.Tile({
	title: "Ocean and Atmospheric Sciences",
	source: new ol.source.TileWMS({
	    url: api_url,
	    params: {
		layers: 'Ocean-Atmosphere',
		transparent: true
	    }
	})
    });							   
    map.addLayer(difOceanAndAtmosphericSciences);
    
    var difOrganismsAndEcosystems = new ol.layer.Tile({
	title: "Organisms and Ecosystems",
	source: new ol.source.TileWMS({
	    url: api_url,
	    params: {
		layers: 'Bio',
		transparent: true
	    }
	})
    });
    map.addLayer(difOrganismsAndEcosystems);
    
    //map.getView().fit([-4103624,-4103624,4103624,4103624], map.getSize());

	var styles = [
        new ol.style.Style({
          stroke: new ol.style.Stroke({
            color: 'rgba(255, 0, 0, 0.2)',
            width: 2
          }),
          fill: new ol.style.Fill({
            color: 'rgba(255, 0, 0, 0.1)'
          })
        }),
		new ol.style.Style({
          image: new ol.style.Circle({
            radius: 4,
           	stroke: new ol.style.Stroke({
	            color: 'rgba(255, 0, 0, 0.2)',
	            width: 2
	          }),
            fill: new ol.style.Fill({
              color: 'rgba(255, 0, 0, 0.1)'
            })
          })
  		})
    ];

	var geom;
	var features = [];
	for (var i in extents) {
		var extent = extents[i];
		var north = extent.north;
		var east = extent.east;
		var west = extent.west;
		var south = extent.south;
	    // point
	    if (west == east && north == south) {
	    	geom = new ol.geom.Point(ol.proj.transform([west, north], 'EPSG:4326', 'EPSG:3031'));
	    	feature = new ol.Feature({
	    			geometry: geom
	    	});
	    	features.push(feature);
	    } else {
	    // box
			geom = new ol.geom.Polygon([[
			  [west, north],
			  [east, north],
			  [east, south],
			  [west, south],
			  [west, north]
			]]).transform('EPSG:4326', 'EPSG:3031');

			[x0, y0] = ol.proj.fromLonLat([west, north], 'EPSG:3031');
			[x1, y1] = ol.proj.fromLonLat([east, south], 'EPSG:3031');
			var xmin = Math.min(x0, x1);
			var xmax = Math.max(x0, x1);
			var ymin = Math.min(y0, y1);
			var ymax = Math.max(y0, y1);


			if (!isNaN(xmin) && !isNaN(xmax) && !isNaN(ymin) && !isNaN(ymax) ){
				// ol.extent.containsExtent(map.getView().calculateExtent(map.getSize()), [xmin, ymin, xmax, ymax])) {
				feature = new ol.Feature({
	    			geometry: geom
	    		});
				features.push(feature);
			}
		}

	}
	console.log(features);
   	var geometries = new ol.layer.Vector({
    	title:"Dataset Geometries",
    	visible: false,
    	source: new ol.source.Vector({
    		features: features
    	}),
    	style: styles
    });
	map.addLayer(geometries);


    var mousePosition = new ol.control.MousePosition({
        coordinateFormat: ol.coordinate.createStringXY(2),
        projection: 'EPSG:4326',
        target: document.getElementById('mouseposition'),
        undefinedHTML: '&nbsp;'
    });
     map.addControl(mousePosition);

    var popup = new ol.Overlay.Popup({"panMapIfOutOfView":false});
    map.addOverlay(popup);
    map.on('click', function(evt) {
	var tolerance = [7, 7];
	var x = evt.pixel[0];
	var y = evt.pixel[1];
	var min_px = [x - tolerance[0], y + tolerance[1]];
	var max_px = [x + tolerance[0], y - tolerance[1]];
	var min_coord = map.getCoordinateFromPixel(min_px);
	var max_coord = map.getCoordinateFromPixel(max_px);
	var min_ll = ol.proj.toLonLat(min_coord, map.getView().getProjection());
	var max_ll = ol.proj.toLonLat(max_coord, map.getView().getProjection());
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
//	if (tracks.getVisible())
//	    layers += ',Entries';

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
				    $(this).attr('src', 'http://www.marine-geo.org/imgs/arrow_show.gif')
				} else if (/arrow_hide/.test($(this).attr('src'))) {
				    $(this).attr('src', 'http://www.marine-geo.org/imgs/arrow_hide.gif')
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

    //this.createLayerSwitcher();
    var layerSwitcher = new ol.control.LayerSwitcher({
        tipLabel: 'Légende'
	//	target: $('#map')[0] // Optional label for button
    });
    map.addControl(layerSwitcher);

    //var zoomControl = new ol.control.Zoom();
    //map.addControl(zoomControl);
    
}

$(document).ready(function() {
    new MapClient();

	// $("#expandMap").on('click', function(e) {
	// 	$("#map").empty();
	// 	if($("#map").width() <= 400) {
	// 		$(".map-btn-container").animate({width:800});
	// 		$(".map-text-container").animate({width:690});
			
	// 		$("#map").animate({
 //      			width: 800,
 //      			height:800
 //    		}, function() {
 //    			$(".map-text-container").css({'padding-top':'14px'});
 //    			new MapClient(2);
 //    			$("#expandMap").html('Shrink Map');
 //    		});
 //    	} else {
 //   			$(".map-btn-container").animate({width:400});
 //   			$(".map-text-container").animate({width:290});
   			
	// 		$("#map").animate({
 //      			width: 400,
 //      			height:400
 //    		}, function() {
 //    			$(".map-text-container").css({'padding-top':'4px'});
 //    			new MapClient(1);
 //    			$("#expandMap").html('Expand Map');
 //    		});
	// 	}
		
 //    });

});


