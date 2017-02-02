(function( $ ){

    // defaultVal and defaultText must be consistent and not collide with
    // the other values in the menu. Other values are assumed to match their text,
    // though this restriction could easily be lifted.
    $.fn.makeDropdownIntoSelect = function(defaultVal, defaultText) {

	var selectedHtml = function(s) {
	    return '<span>' + s + '</span>' + ' <span class="caret"></span>';
	}

	var $val_input = $(this).children().first();
	$(this).find('.dropdown-menu li a').click(function() {
	    var text = $(this).text();
	    var val = text == defaultText ? defaultVal : text;
	    $(this).parents('.dropdown').children('.dropdown-toggle').html(selectedHtml(text));
	    $val_input.val(val);
	    $val_input.change();
	});

	var val = $val_input.val();
	var text = val == defaultVal ? defaultText : val;
	$(this).find('.dropdown-toggle').html(selectedHtml(text));
	
	
	return this;
    }; 
})( jQuery );
