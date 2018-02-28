$(document).ready(function() {
    $('#get-data').click(function() {
	
    });
    new MapClient();



	function MapClient() {
		proj4.defs('EPSG:3031', '+proj=stere +lat_0=-90 +lat_ts=-71 +lon_0=0 +k=1 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs');
	    var projection = ol.proj.get('EPSG:3031');
	    projection.setWorldExtent([-180.0000, -90.0000, 180.0000, -60.0000]);
	    projection.setExtent([-8200000, -8200000, 8200000, 8200000]);

	    var map = new ol.Map({	// set to GMRT SP bounds
		target: 'map',
		interactions: ol.interaction.defaults({mouseWheelZoom:false}),
		view: new ol.View({
		    center: [0, 0],
		    zoom: 1,
		    projection: projection,
		    minZoom: 1,
		    maxZoom: 10
		})
	    });

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

		var geom;

		for (var i in extents) {
			var extent = extents[i];
			var north = extent.north;
			var east = extent.east;
			var west = extent.west;
			var south = extent.south;
		    // point
		    if (west == east && north == south) {
		    	geom = new ol.geom.Point(ol.proj.transform([west, north], 'EPSG:4326', 'EPSG:3031'));
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

				map.getView().fit([xmin, ymin, xmax, ymax], map.getSize());
			}

		    feature = new ol.Feature({
		    	geometry: geom
		    });

		    var layer = new ol.layer.Vector({
		    	source: new ol.source.Vector({
		    		features: [feature]
		    	}),
		    	style: styles
		    });

			map.addLayer(layer);
		}

		var mousePosition = new ol.control.MousePosition({
	        coordinateFormat: ol.coordinate.createStringXY(2),
	        projection: 'EPSG:4326',
	        target: document.getElementById('mouseposition'),
	        undefinedHTML: '&nbsp;'
	    });
	    map.addControl(mousePosition);
	}


});
