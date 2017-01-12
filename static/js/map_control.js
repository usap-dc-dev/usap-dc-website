function MapControl() {

    var self = this;

    var wrapper = $('<div class="map-control-wrapper"/>');
    var tab = $('<div class="map-control-tab"> > </div>');
    var main_contents = $('<div class="map-control-main-contents"><h4>Layers</h4></div>');
    var layersDiv = $('<div class="map-control-layerslist">');

    main_contents.append(layersDiv);
    main_contents.append('<hr/>');
    wrapper.append(tab);
    wrapper.append(main_contents);
    
    var table = $('<table class="map-control-buttons">' +
		  '<tbody>' +

		  '<tr>' +
		    '<td class="map-control-button toggle graticule">' +
		      '<img src="http://www.marine-geo.org/images/grat1.png" alt="Grid" title="Show/Hide Gridlines"/>' +
		    '</td>' +
                    '<td class="map-control-button toggle mask">' +
	    	      '<img src="http://www.marine-geo.org/images/mask.png" alt="Mask" title="Show/Hide GMRT Mask"/>' +
		    '</td>' +
		  '</tr>' +
		  '<tr>' +
		    '<td class="map-control-button toggle mercator">' +
		      '<img src="/images/merc_map_40.png" alt="Mercator" title="Go to Mercator Data Browser">' +
		    '</td>' +
		    '<td class="map-control-button toggle south_polar">' +
		      '<img src="/images/Antarctica-24.png" alt="South Polar" title="Go to South Polar Data Browser"/>' +
		    '</td>' +
		  '</tr>' +
		  
		  '</tbody>' +
		  '</table>');

    this.wrapper = wrapper[0];
    this.layersDiv = layersDiv[0];
    this.table = table[0];
    this.main_contents = main_contents[0];

    main_contents.append(table);
    
    tab.on('click', function(e) {
	if (main_contents.is(":visible")) {
	    main_contents.hide();
	    tab.text("<");
	} else {
	    main_contents.show();
	    tab.text(">");
	}
    });

    main_contents.on('click', '.map-control-button', function() {
	if ($(this).hasClass('on')) {
	    $(this).removeClass('on');
	    var evt = new Event('turn-off');
	    this.dispatchEvent(evt);
	} else {
	    $(this).addClass('on');
	    var evt = new Event('turn-on');
	    this.dispatchEvent(evt);
	}
    });

    $(this.getImgButton('mercator')).on('click', function() {
	window.location.href = "http://" + document.location.host + "/databrowser/";
    });

    $(this.getImgButton('south_polar')).on('click', function() {
	window.location.href = "http://" + document.location.host + "/spdatabrowser/";
    });
}

MapControl.prototype.addLayer = function(name) {
    var item = $('<div class="map-control-layer map-control-button toggle">');
    item.append(name);
    item.attr('title', name);
    $(this.layersDiv).append(item);
}

MapControl.prototype.getLayerButton = function(name) {
    return $(this.layersDiv).find('div.map-control-layer[title="{0}"]'.format(name))[0];
}

MapControl.prototype.getImgButton = function(name) {
    return $(this.table).find('.map-control-button' + '.' + name)[0];
}

MapControl.prototype.layerButtonIsOn = function(name) {
    return $(this.layersDiv).find('div.map-control-layer[title="{0}"]'.format(name)).hasClass('on');
}

MapControl.prototype.imgButtonIsOn = function(name) {
    return $(this.table).find('.map-control-button' + '.' + name).hasClass('on');
}

MapControl.prototype.getElement = function() {
    return this.wrapper;
}
