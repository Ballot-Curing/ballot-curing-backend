function ajaxRequest(params) {
    
    var url = 'https://run.mocky.io/v3/855fe37e-0965-4e90-9cb5-1b233c323c07'
    $.get(url + '?' + $.param(params.data)).then(function (res) {
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
