function MapClient(zoom) {
    if (!zoom) {zoom = 3;}
    proj4.defs('EPSG:3031', '+proj=stere +lat_0=-90 +lat_ts=-71 +lon_0=0 +k=1 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs');
    var projection = ol.proj.get('EPSG:3031');
    projection.setWorldExtent([-180.0000, -90.0000, 180.0000, -60.0000]);
    projection.setExtent([-8200000, -8200000, 8200000, 8200000]);
    var map = new ol.Map({  // set to GMRT SP bounds
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
    this.map = map;

    var api_url = $("#mapserver").text();
  
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
        "title":"Antarctic Ice Sheet Thickness (British Antarctic Survey)"
    };
    displayXBMap(map, ice_thickness);

    var ice_vel = {
        "source":"https://d1ubeg96lo3skd.cloudfront.net/data/basemaps/images/antarctic/AntarcticIceVelocity_320",
        "numLevels":4,
        "title":"Antarctic Ice Sheet Velocity (Rignot et al, 2014)"
    };
    displayXBMap(map, ice_vel);

    var bedrock = {
        "source":"https://www.thwaites-explorer.org/data/BeneathAntarcticIcesheet/Bedmachine_bed_ETOPO1_clipped_600m_zoom8",
        "mapMaxZoom":8,
        "mapMaxResolution":150.00535457,
        "tileWidth":256,
        "tileHeight":256,
        "extent":[-2719812.53530000, -2372227.21632370, 2939589.48209285, 2465145.45800000],
        "title":"Bedrock Elevation Under the Antarctic Ice Sheet (Morlighem, M. 2019)"
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

    var amrcSource = new ol.source.Vector({
        format: new ol.format.GeoJSON(),
        loader: function(){
            $.get({
                url: '/static/data/amrc_stations.csv' ,
                dataType: 'text'
            }).done(function(response) {
                var csvString = response;
                csv2geojson.csv2geojson(csvString, {latfield:'y', lonfield:'x'}, function(err, data) {
                    amrcSource.addFeatures(vectorSource.getFormat().readFeatures(data, {dataProjection: 'EPSG:4326', featureProjection:'EPSG:3031'}))
                });
            });
        }
    });
    var amrc_stations =  new ol.layer.Vector({
        visible: false,
        source: amrcSource,
        style: [
            new ol.style.Style({
              image: new ol.style.Circle({
                radius: 5,
                stroke: new ol.style.Stroke({
                    color: 'rgba(255, 0, 0, 0.6)',
                    width: 1
                  }),
                fill: new ol.style.Fill({
                  color: 'rgba(255, 255, 255, 0.3)'
                })
              })
            })
        ],
        title: 'AMRC Weather Stations',
    });  
    map.addLayer(amrc_stations);

    var iodpSource = new ol.source.Vector({
        format: new ol.format.GeoJSON(),
        loader: function() {
            $.get({
                url: "/static/data/Drilled_Holes_Filtered.csv",
                dataType: "text"
            }).done(function(response){
                var csvString = response;
                csv2geojson.csv2geojson(csvString, {latfield: "latitude", lonfield: "longitude"}, function(err, data) {
                    iodpSource.addFeatures(vectorSource.getFormat().readFeatures(data, {dataProjection: 'EPSG:4326', featureProjection: 'EPSG:3031'}));
                })
            });
        }
    });
    var iodpStations = new ol.layer.Vector({
        visible: false,
        source: iodpSource,
        style: [
            new ol.style.Style({
                image: new ol.style.Circle({
                    radius: 5,
                    stroke: new ol.style.Stroke({
                        color: 'rgba(0, 0, 0, 0.6)',
                        width: 1
                    }),
                    fill: new ol.style.Fill({
                        color: 'rgba(0, 80, 80, 0.6)'
                    })
                })
            })
        ],
        title: "IODP Drill Sites"
    });
    map.addLayer(iodpStations);

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
            radius: 8,
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

    var mousePosition = new ol.control.MousePosition({
        coordinateFormat: ol.coordinate.createStringXY(2),
        projection: 'EPSG:4326',
        target: document.getElementById('mouseposition'),
        undefinedHTML: '&nbsp;'
    });
    map.addControl(mousePosition);


    var geometries;

    var popup = new ol.Overlay.Popup({"panMapIfOutOfView":false});

    map.addOverlay(popup);


    map.on('click', function(evt) {
        map.removeLayer(geometries);
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
    //  if (tracks.getVisible())
    //      layers += ',Entries';

        if (layers.length > 0) {
            $.ajax({
                type: "GET",
                url: window.location.protocol + '//' + window.location.hostname + '/getfeatureinfo?', //"http://www.usap-data.org/usap_layers.php"
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
        
                        //get a list of dataset ids from the returned msg
                        var regex = /view\/dataset\//gi, result, ids = [];
                        while ( (result = regex.exec(msg)) ) {
                            id = msg.substring(result.index+13, result.index+19);
                            ids.push(id);
                        }

                        //plot the geometries for each dataset listed
                        var geom;
                        var features = [];
                        for (var i in extents) {
                            var extent = extents[i];
                            if (ids.includes(extent.dataset_id)) {

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
                                    var polygon = [];
                                    var n = 30.0;
                                    var dlon, dlat;
                                    if (extent.cross_dateline){
                                        dlon = (-180 - west)/n;
                                        dlat = (north - south)/n;
                                        for (i = 0; i < n; i++) {
                                            polygon.push([-180-dlon*i, north]);
                                        }
                                        for (i = 0; i < n; i++) {
                                            polygon.push([west, north-dlat*i]);
                                        }
                                        for (i = 0; i < n; i++) {
                                            polygon.push([west+dlon*i, south]);
                                        }
                                        dlon = (180 - east)/n;
                                        for (i = 0; i < n; i++) {
                                            polygon.push([180-dlon*i, south]);
                                        }
                                        for (i = 0; i < n; i++) {
                                            polygon.push([east, south+dlat*i]);
                                        }
                                        for (i = 0; i < n; i++) {
                                            polygon.push([east+dlon*i, north]);
                                        }
                                    }
                                    else if (east > west) {
                                        dlon = (west - east)/n;
                                        dlat = (north - south)/n;
                                        for (i = 0; i < n; i++) {
                                            polygon.push([west-dlon*i, north]);
                                        }
                                        for (i = 0; i < n; i++) {
                                            polygon.push([east, north-dlat*i]);
                                        }
                                        for (i = 0; i < n; i++) {
                                            polygon.push([east+dlon*i, south]);
                                        }
                                        for (i = 0; i < n; i++) {
                                            polygon.push([west, south+dlat*i]);
                                        }
                                    } 

                                    else {
                                        dlon = (-180 - east)/n;
                                        dlat = (north - south)/n;
                                        for (i = 0; i < n; i++) {
                                            polygon.push([-180-dlon*i, north]);
                                        }
                                        for (i = 0; i < n; i++) {
                                            polygon.push([east, north-dlat*i]);
                                        }
                                        for (i = 0; i < n; i++) {
                                            polygon.push([east+dlon*i, south]);
                                        }
                                        dlon = (180 - west)/n;
                                        for (i = 0; i < n; i++) {
                                            polygon.push([180-dlon*i, south]);
                                        }
                                        for (i = 0; i < n; i++) {
                                            polygon.push([west, south+dlat*i]);
                                        }
                                        for (i = 0; i < n; i++) {
                                            polygon.push([west+dlon*i, north]);
                                        }
                                    }

                                    geom = new ol.geom.Polygon([polygon]).transform('EPSG:4326', 'EPSG:3031');

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
                        }
                        geometries = new ol.layer.Vector({
                            visible: true,
                            source: new ol.source.Vector({
                                features: features
                            }),
                            style: styles
                        });
                        map.addLayer(geometries);


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
                        var closer = document.getElementsByClassName('ol-popup-closer')[0];
                        closer.onclick = function() {
                            map.removeLayer(geometries);
                        };


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

        //check for other features
        var icecore_features = [];
        var amrc_features = [];
        var iodp_drillsites = [];
        if (evt.pixel) {
            map.forEachFeatureAtPixel(evt.pixel, 
                function(feature, layer) {
                    if (layer && layer.getProperties()){
                        if (layer.getProperties().title === 'Ice Cores - (NSF ice core facility)') {
                            icecore_features.push(feature);
                        }
                        else if (layer.getProperties().title === 'AMRC Weather Stations') {
                            amrc_features.push(feature);
                        }
                        else if(layer.getProperties().title === 'IODP Drill Sites') {
                            iodp_drillsites.push(feature);
                        }
                    }
                }, {"hitTolerance":1});
        }
        if (icecore_features.length > 0) {
            var msg = "<h6>Number of ice cores: " + icecore_features.length + "</h6>";
            for (let feature of icecore_features) {
                var props = feature.getProperties();
                var fs_url = "https://icecores.org/inventory/"+props['Field Site'].replaceAll(' ', '-').toLowerCase();
                // make pop up
                msg += '<p style="font-size:0.8em;max-height:500px;"><b>Core Name:</b> ' + props['Core Name'];
                msg += '<br><b>Field Site:</b><a href="' + fs_url + '" target="_blank"> ' + props['Field Site'] + '</b></a>';
                msg += '<br><b>Years Drilled:</b> ' + props['Years Drilled'];
                msg += '<br><b>Original PI:</b> ' + props['Original PI'];
                msg += '<br><b>De-accessed</b> ' + props['De-accessed'];
                msg += '<br>===========<br> </p>';
            }
            var $msg = $('<div>' + msg + '</div>');
            popup.show(evt.coordinate, $msg.prop('innerHTML'));
        }
        if (amrc_features.length > 0) {
            var msg = "<h6>Number of weather stations: " + amrc_features.length + "</h6>";
            for (let feature of amrc_features) {
                var props = feature.getProperties();
                // make pop up
                msg += '<p style="font-size:0.8em;max-height:500px;">';
                msg += '<b>Name:</b><a href="' + props['Repo Link'] + '" target="_blank"> ' + props['Name'] + '</b></a>';
                msg += '<br><b>Region:</b> ' + props['Region'];
                msg += '<br><b>Elevation:</b> ' + props['Elevation'];
                msg += '<br><b>Lat/Long</b> ' + props['Lat/Long'];
                msg += '<br>===========<br> </p>';
            }
            var $msg = $('<div>' + msg + '</div>');
            popup.show(evt.coordinate, $msg.prop('innerHTML'));
        }
        if(iodp_drillsites.length > 0) {
            var msg = "<h6>Number of drill sites: " + iodp_drillsites.length + "</h6>";
            for(let drillSite of iodp_drillsites) {
                var props = drillSite.getProperties();
                msg += '<p style="font-size:0.8em; max-height: 500px;">';
                if(props["leg"] && props["leg"].length>0) {
                    msg += "<b>Leg:</b> " + Number(props['leg']) + "<br>";
                }
                if(props['expedition'] && props['expedition'].length>0) {
                    msg += "<b>Expedition:<b> " + props['expedition'] + "<br>";
                }
                msg += "<b>Site:</b> " + props['site'] + "<br>";
                msg += "<b>Link to Core Data:</b> " + '<a href="' + props['url_core'] + '" target="_blank">' + "URL core" + '</a><br>'
                if(props["arrival"] && props["arrival"].length>0) {
                    msg += "<b>Arrival:</b> " + props["arrival"] + "<br>"
                }
                msg += "===========<br>"
            }
            var $msg = $('<div>' + msg + '</div>');
            popup.show(evt.coordinate, $msg.prop('innerHTML'));
        }
    });

    //drawing buttons
    this.vectorSrc = new ol.source.Vector();
    var vectorLayer = new ol.layer.Vector({ source: this.vectorSrc });
    this.map.addLayer(vectorLayer);
    var self = this;
    $('.drawing-button').on('click', function(e) {
        var mode = $(this).attr('data-mode');
        self.setDrawMode(mode);
        self.setDrawingTool(mode);
    });

    $('#clear-button').click(function() {
        self.vectorSrc.clear();
    });

    $('#search-button').click(function() {
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


    //this.createLayerSwitcher();
    var layerSwitcher = new ol.control.LayerSwitcher({
        tipLabel: 'Légende'
    //  target: $('#map')[0] // Optional label for button
    });
    map.addControl(layerSwitcher);

    //var zoomControl = new ol.control.Zoom();
    //map.addControl(zoomControl);
    
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
        console.log(coords);
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



$(document).ready(function() {
    extents = JSON.parse($("#div_extents").text());
    mc = new MapClient();
});
