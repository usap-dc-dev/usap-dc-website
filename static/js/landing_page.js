$(document).ready(function() {
    $('[data-toggle="popover"]').popover({html: true, container: 'body', trigger:"hover"});

    $('#get-data').click(function() {
	
    });
    
    extents = JSON.parse($("#div_extents").text());

    new MapClient();

	function MapClient() {
		proj4.defs('EPSG:3031', '+proj=stere +lat_0=-90 +lat_ts=-71 +lon_0=0 +k=1 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs');
	    var projection = ol.proj.get('EPSG:3031');
	    projection.setWorldExtent([-180.0000, -90.0000, 180.0000, -60.0000]);
	    projection.setExtent([-8200000, -8200000, 8200000, 8200000]);
		var api_url = 'https://api.usap-dc.org:8443/wfs?';

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
		    url: "https://www.gmrt.org/services/mapserver/wms_SP?request=GetCapabilities&service=WMS&version=1.3.0",
		    params: {
			layers: 'South_Polar_Bathymetry'
		    }
		})
	    });
	    map.addLayer(gmrt);

		var ibcso = new ol.layer.Tile({
			type: 'base',
			title: "IBCSO",
			visible: false,
			source: new ol.source.TileArcGISRest({
				url:"https://gis.ngdc.noaa.gov/arcgis/rest/services/antarctic/antarctic_basemap/MapServer",
				crossOrigin: 'anonymous',
				params: {
					transparent: true
				}
			})
		  });
		map.addLayer(ibcso);
	
	
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
	
		
		var rema = new ol.layer.Tile({
			title: "REMA 8m",
			visible: false,
			source: new ol.source.TileArcGISRest({
				url: 'https://elevation2.arcgis.com/arcgis/rest/services/Polar/AntarcticDEM/ImageServer',
				params: {
					transparent: true
				}
			})
		});
		map.addLayer(rema);
	
		var gmrtmask = new ol.layer.Tile({
			visible: false,
			title: "GMRT Synthesis-Mask",
			source: new ol.source.TileWMS({
				url: "https://www.gmrt.org/services/mapserver/wms_SP_mask?request=GetCapabilities&service=WMS&version=1.3.0",
				params: {
				layers: 'South_Polar_Bathymetry'
				}
			})
			});
		map.addLayer(gmrtmask);
	
		var tracks = new ol.layer.Tile({
			title: "USAP R/V Cruises",
			visible: false,
			source: new ol.source.TileWMS({
				url: "https://www.marine-geo.org/services/mapserver/wmscontrolpointsSP?",
				params: {
				layers: 'Tracks-Lines',
				transparent: true
				}
			})
			});
		map.addLayer(tracks);
	
	
		var arffsu_core = new ol.layer.Tile({
			title: 'Former FSU sediment core map',
			visible: false,
			source: new ol.source.TileWMS({
			  url: 'https://gis.ngdc.noaa.gov/arcgis/services/Sample_Index/MapServer/WMSServer?',
			//   crossOrigin: 'anonymous',
			  projection: 'EPSG:3031',
			  params: {
				  layers: "ARFFSU"
			  }
			})
		});
		map.addLayer(arffsu_core);
	
		var bpcrr_core = new ol.layer.Tile({
			title: 'BPC Rock Repository - samples',
			visible: false,
			source: new ol.source.TileWMS({
			  url: 'https://gis.ngdc.noaa.gov/arcgis/services/Sample_Index/MapServer/WMSServer?',
			//   crossOrigin: 'anonymous',
			  projection: 'EPSG:3031',
			  params: {
				  layers: "BPCRR"
			  }
			})
		});
		map.addLayer(bpcrr_core);
	
		var asdl = new ol.layer.Tile({
			title: 'SDLS - seismic track lines',
			visible: false,
			source: new ol.source.TileWMS({
			  url: 'https://sdls.ogs.trieste.it/geoserver/ows?version=1.1.0',
			  crossOrigin: 'anonymous',
			  params: {
				  layers: "AllNavigations20201127"
			  }
			})
		});
		map.addLayer(asdl);
	
		var ice_thickness = {
			"source":"https://d1ubeg96lo3skd.cloudfront.net/data/basemaps/images/antarctic/AntarcticIcesheetThickness_320",
			"numLevels":2,
			"title":"Antarctic Ice Sheet Thickness"
		};
		displayXBMap(map, ice_thickness);
	
		var ice_vel = {
			"source":"https://d1ubeg96lo3skd.cloudfront.net/data/basemaps/images/antarctic/AntarcticIceVelocity_320",
			"numLevels":4,
			"title":"Antarctic Ice Sheet Velocity"
		};
		displayXBMap(map, ice_vel);
	
		var bedrock = {
			"source":"https://www.thwaites-explorer.org/data/BeneathAntarcticIcesheet/Bedmachine_bed_ETOPO1_clipped_600m_zoom8",
			"mapMaxZoom":8,
			"mapMaxResolution":150.00535457,
			"tileWidth":256,
			"tileHeight":256,
			"extent":[-2719812.53530000, -2372227.21632370, 2939589.48209285, 2465145.45800000],
			"title":"Bedrock Elevation"
		};
		displayTiled(map, bedrock);
	 
	
	
		var iceCoreSource = new ol.source.Vector({
			format: new ol.format.GeoJSON(),
			loader: function(){
				$.get({
					url: '/static/data/ice_cores.csv' ,
					dataType: 'text'
				}).done(function(response) {
					var csvString = response;
					csv2geojson.csv2geojson(csvString, function(err, data) {
						iceCoreSource.addFeatures(vectorSource.getFormat().readFeatures(data, {dataProjection: 'EPSG:4326', featureProjection:'EPSG:3031'}))
					});
				});
			}
		});
		var ice_cores =  new ol.layer.Vector({
			visible: false,
			source: iceCoreSource,
			// style: styleFunction,
			title: 'Ice Cores - (NSF ice core facility)',
		});  
		map.addLayer(ice_cores);
	
		var vectorSource = new ol.source.Vector({
			format: new ol.format.GeoJSON(),
			loader: function(){
				$.get({
					url: 'https://www.thwaites-explorer.org/data/antarctic_coastS10RS/Antarctic_coastS10polyRS.geojson' ,
					dataType: 'json'
				}).done(function(data) {
					vectorSource.addFeatures(vectorSource.getFormat().readFeatures(data, {dataProjection: 'EPSG:4326', featureProjection:'EPSG:3031'}))
				});
			}
		});
		var coastline =  new ol.layer.Vector({
			visible: true,
			source: vectorSource,
			style: styleFunctionOutline,
			title: 'Antarctic Coast',
		});  
		map.addLayer(coastline)
	
	
		var names = new ol.layer.Tile({
			title: "Place Names",
			visible: true,
			source: new ol.source.TileArcGISRest({
				url:"https://gis.ngdc.noaa.gov/arcgis/rest/services/antarctic/reference/MapServer",
				params: {
					transparent: true
			  }
			})
		  });
		map.addLayer(names);
	    
	    var difINT = new ol.layer.Tile({
		title: "Integrated System Science",
		visible: false,
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
		visible: false,
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
		visible: false,
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
		visible: false,
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
		visible: false,
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
		visible: false,
		source: new ol.source.TileWMS({
		    url: api_url,
		    params: {
			layers: 'Bio',
			transparent: true
		    }
		})
	    });
	    map.addLayer(difOrganismsAndEcosystems);

	    var layerSwitcher = new ol.control.LayerSwitcher({
	        tipLabel: 'Légende'
	    });
	    map.addControl(layerSwitcher);


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
		var xmin = 8200000;
		var xmax = -8200000;
		var ymin = 8200000;
		var ymax = -8200000;

		function updateBounds(lon, lat) {
			[x, y] = ol.proj.fromLonLat([lon, lat], 'EPSG:3031');
	    	xmin = Math.min(x, xmin);
			xmax = Math.max(x, xmax);
			ymin = Math.min(y, ymin);
			ymax = Math.max(y, ymax);
		}

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
		    	var polygon = [];
		    	var n = 30;
		    	var dlon, dlat;

			    if (extent.cross_dateline){
			    	dlon = (-180 - west)/n;
			    	dlat = (north - south)/n;
			    	for (i = 0; i < n; i++) {
			    		polygon.push([-180-dlon*i, north]);
			    		updateBounds(-180-dlon*i, north);
			    	}
			    	for (i = 0; i < n; i++) {
			    		polygon.push([west, north-dlat*i]);
			    		updateBounds(west, north-dlat*i);
			    	}
			    	for (i = 0; i < n; i++) {
			    		polygon.push([west+dlon*i, south]);
			    		updateBounds(west+dlon*i, south);
			    	}
			    	dlon = (180 - east)/n;
			    	for (i = 0; i < n; i++) {
			    		polygon.push([180-dlon*i, south]);
			    		updateBounds(180-dlon*i, south);
			    	}
			    	for (i = 0; i < n; i++) {
			    		polygon.push([east, south+dlat*i]);
			    		updateBounds(east, south+dlat*i);
			    	}
			    	for (i = 0; i < n; i++) {
			    		polygon.push([east+dlon*i, north]);
			    		updateBounds(east+dlon*i, north);
			    	}
			   	}
		    	else if (east > west) {
			    	dlon = (west - east)/n;
			    	dlat = (north - south)/n;
			    	for (i = 0; i < n; i++) {
			    		polygon.push([west-dlon*i, north]);
			    		updateBounds(west-dlon*i, north);
			    	}
			    	for (i = 0; i < n; i++) {
			    		polygon.push([east, north-dlat*i]);
			    		updateBounds(east, north-dlat*i);
			    	}
			    	for (i = 0; i < n; i++) {
			    		polygon.push([east+dlon*i, south]);
			    		updateBounds(east+dlon*i, south);
			    	}
			    	for (i = 0; i < n; i++) {
			    		polygon.push([west, south+dlat*i]);
			    		updateBounds(west, south+dlat*i);
			    	}
			    } 

			    else {
			    	dlon = (-180 - east)/n;
			    	dlat = (north - south)/n;
			    	for (i = 0; i < n; i++) {
			    		polygon.push([-180-dlon*i, north]);
			    		updateBounds(-180-dlon*i, north);
			    	}
			    	for (i = 0; i < n; i++) {
			    		polygon.push([east, north-dlat*i]);
			    		updateBounds(east, north-dlat*i);
			    	}
			    	for (i = 0; i < n; i++) {
			    		polygon.push([east+dlon*i, south]);
			    		updateBounds(east+dlon*i, south);
			    	}
			    	dlon = (180 - west)/n;
			    	for (i = 0; i < n; i++) {
			    		polygon.push([180-dlon*i, south]);
			    		updateBounds(180-dlon*i, south);
			    	}
			    	for (i = 0; i < n; i++) {
			    		polygon.push([west, south+dlat*i]);
			    		updateBounds(west, south+dlat*i);
			    	}
			    	for (i = 0; i < n; i++) {
			    		polygon.push([west+dlon*i, north]);
			    		updateBounds(west+dlon*i, north);
			    	}
			    }
				geom = new ol.geom.Polygon([polygon]).transform('EPSG:4326', 'EPSG:3031');

				if (!isNaN(xmin) && !isNaN(xmax) && !isNaN(ymin) && !isNaN(ymax) &&
					ol.extent.containsExtent(map.getView().calculateExtent(map.getSize()), [xmin, ymin, xmax, ymax])) {
					map.getView().fit([xmin, ymin, xmax, ymax], map.getSize());
				}
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
