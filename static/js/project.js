$(document).ready(function() {
    $('#award').change(function() {
	var title = pi = institution = email = copi = start = end = cr = ipy = null;
	var val = $('#award').val();
	if (val != '0') {
	    var award_num = val.split(' ')[0];
	    $.ajax({
		method: 'GET',
		url: 'http://www.usap-dc.org/submit/projectinfo?award='+award_num,
		success: function(msg) {
		    console.log(msg);
		    $("#entry textarea[name='title']").val(msg.title);
		    $("#entry input[name='name']").val(msg.name);
		    $("#entry textarea[name='org']").val(msg.org);
		    $("#entry input[name='email']").val(msg.email);
		    $("#entry input[name='copi']").val(msg.copi);
		    $("#entry input[name='start']").val(msg.start);
		    $("#entry input[name='end']").val(msg.expiry);
		    $("#entry input[name='iscr']").prop('checked',msg.iscr);
		    $("#entry input[name='isipy']").prop('checked',msg.isipy);
		    $("#entry textarea[name='sum']").val(msg.sum);
		}
	    });
	}
	
    });

    $('#add-location').click(function() {
	var $cloned = $("#locations select[name='location1']").clone();
	var idx = $('#locations').children().length+1;
	$cloned.attr('name','location'+idx);
	$cloned.attr('id', 'location'+idx);
	$cloned.children().first().text('(Select Term ' + idx + ')');
	$('#locations').append($cloned);
	
    });

    $('#add-parameter').click(function() {
	var $cloned = $("#parameters select[name='parameter1']").clone();
	var idx = $('#parameters').children().length+1;
	$cloned.attr('name','parameter'+idx);
	$cloned.attr('id', 'parameter'+idx);
	$cloned.children().first().text('(Select Term ' + idx + ')');
	$('#parameters').append($cloned);	
    });

    $('#add_repo').click(function() {
	var repos = $('#repositories');
	var idx = repos.length+1
	var new_repo = $('#repo1').clone();
	new_repo.attr('id', 'repo'+idx);
	new_repo.find('input[name="repo1"]').attr('name','repo'+idx).prop('checked', false);
	new_repo.find('input[name="repo_name_other1"]').attr('name','repo_name_other'+idx);
	new_repo.find('input[name="repo_id_other1"]').attr('name', 'repo_id_other'+idx);
	repos.append('<hr>');
	repos.append(new_repo);
    });

    $('input[name="repos"]').change(function() {
	if ($(this).prop('checked')) {
	    $("#repositories input[type='radio']").prop('checked', false);
	    $("#repositories input[type='text']").val('');
	}
    });

    $("#repositories input[type='radio']").change(function() {
	if ($(this).prop('checked')) {
	    $('input[name="repos"]').prop('checked', false);
	}
    });

});
