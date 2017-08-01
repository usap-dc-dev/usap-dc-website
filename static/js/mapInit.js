function MapClient() {
    proj4.defs('EPSG:3031', '+proj=stere +lat_0=-90 +lat_ts=-71 +lon_0=0 +k=1 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs');
    var projection = ol.proj.get('EPSG:3031');
    projection.setWorldExtent([-180.0000, -90.0000, 180.0000, -60.0000]);
    projection.setExtent([-8200000, -8200000, 8200000, 8200000]);
    var map = new ol.Map({	// set to GMRT SP bounds
	target: 'map',
	view: new ol.View({
	    center: [0,0],
	    zoom: 1,
	    projection: projection,
	    minZoom: 1,
	    maxZoom: 10
	}),
	
    });

    var api_url = 'http://api.usap-dc.org:81/wfs?'

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

    var modis = new ol.layer.Tile({
	type: 'base',
	title: "MODIS Mosaic",
	visible: false,
	source: new ol.source.TileWMS({
	    url: api_url,
	    params: {
		layers: 'MODIS'
	    }
	})
    });
    map.addLayer(modis);
    
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
	title: "Antarctic Integrated System Science",
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
	title: "Antarctic Earth Sciences",
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
	title: "Antarctic Astrophysics and Geospace Sciences",
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
	title: "Antarctic Glaciology",
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
	title: "Antarctic Ocean and Atmospheric Sciences",
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
	title: "Antarctic Organisms and Ecosystems",
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

    var mousePosition = new ol.control.MousePosition({
        coordinateFormat: ol.coordinate.createStringXY(2),
        projection: 'EPSG:4326',
        target: document.getElementById('mouseposition'),
        undefinedHTML: '&nbsp;'
    });
    map.addControl(mousePosition);


    var popup = new ol.Overlay.Popup();
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
//	if (tracks.getVisible())
//	    layers += ',Entries';
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
		    var $msg = $('<div>'+msg+'</div>');
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
			    isrc = 'http://www.marine-geo.org/imgs/arrow_show.gif'
			$(this).find('img').attr('src',isrc);
			$(this).parent().find('.tcontent').toggle();
			//map.popups[0].updateSize();
		    });
		}
	    }
	});
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
});
