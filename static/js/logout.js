$.ajax({
    url: 'https://orcid.org/userStatus.json?logUserOut=true',
    dataType: 'jsonp',
    success: function(result,status,xhr) {
      console.log("Logged out of Orcid");
    },
    error: function (xhr, status, error) {
      console.log(status);
    }
});

