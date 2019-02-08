$(document).ready(function() {

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
  
  $('.map-btn').click(function(event) {
    $("#geometry_title").text('Spatial Bounds');
    plotGeometry(map, styles);
    var x = event.pageX;
    var y = event.pageY;
    $("#geometry").css({top:y-450+"px", left:x+"px"}).show();
  });

  $('.close_geom_btn').click(function() {
    $("#geometry").hide();
  });
  

  //Make the DIV element draggagle:
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
        url: "http://api.usap-dc.org:81/wfs?",
        params: {
        layers: "LIMA 240m",
        transparent: true
        }
    })
  });
  map.addLayer(lima);

  return map;
}

function plotGeometry(map, styles) {

  //first remove previous geometries
  removeLayerByName(map, "geometry");

  var north = parseFloat($("#geo_n").val());
  var east = parseFloat($("#geo_e").val());
  var west = parseFloat($("#geo_w").val());
  var south = parseFloat($("#geo_s").val());

  if (isNaN(north) || isNaN(east) || isNaN(west) || isNaN(south)) return;
  var cross_dateline = $("#cross_dateline").is(':checked');
  // point
  if (west == east && north == south) {
    geom = new ol.geom.Point(ol.proj.transform([west, north], 'EPSG:4326', 'EPSG:3031'));
  } else {
  // polygon
    var polygon = [];
    var n = 30.0;
    var dlon, dlat;

    if (cross_dateline){
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
  }


  var feature = new ol.Feature({
      geometry: geom
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
