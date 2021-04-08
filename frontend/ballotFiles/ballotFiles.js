function ajaxRequest(params) {
    console.log(params)
    var url = 'http://128.220.221.36:5500/api/v1/ballots/?state=GA&election_dt=01-04-2021'
    $.get(url).then(function (res) {
        params.success(res)
    })
}

$(document).ready(function(){
    $("#myInput").on("keyup", function() {
      var value = $(this).val().toLowerCase();
      $(".dropdown-menu li").filter(function() {
        $(this).toggle($(this).text().toLowerCase().indexOf(value) > -1)
      });
    });
  });
