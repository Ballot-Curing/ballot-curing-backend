function ajaxRequest(params) {
    console.log($("#dropdownState").text())
    var url = 'http://128.220.221.36:5500/api/v1/ballots/?state=GA&election_dt=01-04-2021'
    $.get(url).then(function (res) {
        params.success(res)
    })
}

function dropdownToggle() {
  // select the main dropdown button element
  var dropdown = $(this).parent().parent().prev();

  // change the CONTENT of the button based on the content of selected option
  dropdown.html($(this).html());

  // change the VALUE of the button based on the data-value property of selected option
  dropdown.val($(this).prop('data-value'));

  //console.log($("#dropdownState").text());

}



$(document).ready(function(){
    
  $("#table").hide();
  $("#downloadBtn").hide();

  $('.dropdown-menu a').on('click', dropdownToggle);
  
  //console.log($("#dropdownState").val());
  var state_val = "";
  var election_val = "";
  

  $("#enterBtn").on("click", function() {
    state_val = $("#dropdownState").text();
    election_val = $("#dropdownElection").text();
    console.log(state_val);
    console.log(election_val);
    $("#table").show();
    $.ajax({
      type: 'GET',
      url: 'http://128.220.221.36:5500/api/v1/ballots/',
      dataType: 'json',
      data: {state:state_val, election_dt:election_val},
      success: function (response) {
        console.log(response);
        $.each(response, function(i, item){
          $("#table tbody").append(
            "<tr>"
                +"<td>"+item.county+"</td>"
                +"<td>"+item.voter_reg_id+"</td>"
                +"<td>"+item.city+"</td>"
                +"<td>"+item.state+"</td>"
                +"<td>"+item.zip+"</td>"
                +"<td>"+item.ballot_rtn_status+"</td>"
                +"<td>"+item.ballot_issue+"</td>"
            +"</tr>" )
        })
       },
       error: function (e) {
           $("#divResult").html("WebSerivce unreachable");
       }
    });
    $("#downloadBtn").show();

  });



  
  
  $("#myInput").on("keyup", function() {
      var value = $(this).val().toLowerCase();
      $(".dropdown-menu li").filter(function() {
        $(this).toggle($(this).text().toLowerCase().indexOf(value) > -1)
      });
    });
  });
