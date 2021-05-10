function displayXBMap(thismap, overlay) {
    //calculate the resolutions for each zoom level
    var projExtent = thismap.getView().getProjection().getExtent();
    if (overlay.extent) projExtent = overlay.extent;
    var startResolution = ol.extent.getWidth(projExtent) / 320;
    var resolutions = new Array(overlay.numLevels+1);
    for (var i = 0, ii = resolutions.length; i < ii; ++i) {
      resolutions[i] = startResolution / Math.pow(2, i);
    } 
  
    //set up a tile grid
    tileGrid = new ol.tilegrid.TileGrid({
      minZoom: 1,
      maxZoom: overlay.numLevels,
      extent: projExtent,
      resolutions: resolutions,
      tileSize: [320,320]
    });
  
    var source_url = overlay.source;
    //make sure there is not a / and the end of the source URL
    if (source_url.slice(-1) == "/") {
      source_url = source_url.slice(0, -1);
    }
    //make sure there is not a / and the end of the source URL
    if (source_url.slice(-1) == "/") {
      source_url = source_url.slice(0, -1);
    }
    var xbMapUrlTemplate =  source_url + '/i_{res}/{name}.png'; 
    var xbMapLayer = new ol.layer.Tile({
      source: new ol.source.XYZ({
        projection: thismap.getView().getProjection(),
        tileSize: 320,
        minZoom:1,
        maxZoom:overlay.numLevels,
        tileGrid: tileGrid,
        tileUrlFunction: function(tileCoord) {
          var url = xbMapUrlTemplate.replace('{res}', (Math.pow(2,tileCoord[0]-1)).toString())
              .replace('{name}', getNameWithTileX(tileCoord));
          return(url);
        },
        wrapX: false
      }),
      title: overlay.title,
      visible: false
    });

    thismap.addLayer(xbMapLayer);

    //use the tileCoords to geerate the URL name
    function getNameWithTileX(tileCoord) {
      var x = tileCoord[1];
      var y = tileCoord[2];
      var res = Math.pow(2,tileCoord[0]-1);
  
      var first;
      var second;
      if (res - x > 0)
        first = "W" + (res-x).toString();
      else
        first = "E" + (x-res).toString();
  
      if (res + y >= 0 )
        second = "S" + (res + 1 + y).toString();
      else
        second = "N" + (-1 * (res + 1 + y)).toString();
  
      var name = first + second + "_320";
      return name;
    }
}

function displayTiled(thismap, overlay) {
    var mapResolutions = [];
    for (var z = 0; z <= overlay.mapMaxZoom; z++) {
      mapResolutions.push(Math.pow(2, overlay.mapMaxZoom - z) * overlay.mapMaxResolution);
    }
  
    var mapTileGrid = new ol.tilegrid.TileGrid({
      tileSize: [overlay.tileWidth, overlay.tileHeight],
      extent: overlay.extent,
      minZoom: 1,
      resolutions: mapResolutions
    });
  
    var layer = new ol.layer.Tile({
      source: new ol.source.XYZ({
        projection: 'EPSG:3031',
        tileGrid: mapTileGrid,
        tilePixelRatio: 1.00000000,
        url: overlay.source + "/{z}/{x}/{y}.png",
      }),
      title: overlay.title,
      visible: false
    });

    thismap.addLayer(layer);
}

var styleFunctionOutline = function(feature, resolution){
    var context = {
        feature: feature,
        variables: {}
    };
    var value = "";
    var labelText = "";
    size = 0;
    var labelFont = "10px, sans-serif";
    var labelFill = "rgba(0, 0, 0, 1)";
    var bufferColor = "";
    var bufferWidth = 0;
    var textAlign = "left";
    var textBaseline = "top";
    var offsetX = 8;
    var offsetY = 3;
    var placement = 'point';
    var color = 'rgba(35,35,35,1.0)';
    if (feature.get('stroke_color')) color = feature.get('stroke_color');
    var width = 0;
    if (feature.get('stroke_width')) width = feature.get('stroke_width');
    if ("" !== null) {
        labelText = String("");
    }
    var style = [ new ol.style.Style({
        stroke: new ol.style.Stroke({color: color, lineDash: null, lineCap: 'square', lineJoin: 'bevel', width: width}),
        text: createTextStyle(feature, resolution, labelText, labelFont,
                              labelFill, placement, bufferColor, offsetX, offsetY, textBaseline,
                              bufferWidth, textAlign)
    })];

    return style;
};

// text style function for geojson layers
var createTextStyle = function(feature, resolution, labelText, labelFont,
    labelFill, placement, bufferColor, offsetX, offsetY, textBaseline,
    bufferWidth, textAlign) {

        if (feature.hide || !labelText) {
            return; 
        } 
        var bufferStyle = null;
        if (bufferWidth == 0) {
            bufferStyle = null;
        } else {
        bufferStyle = new ol.style.Stroke({
            color: bufferColor,
            width: bufferWidth
        });
    }

    var textStyle = new ol.style.Text({
        font: labelFont,
        text: labelText,
        textBaseline: textBaseline,
        textAlign: textAlign,
        offsetX: offsetX,
        offsetY: offsetY,
        placement: placement,
        maxAngle: 1,
        fill: new ol.style.Fill({
            color: labelFill
        }),
        stroke: bufferStyle
    });

    return textStyle;
};