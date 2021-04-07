$(document).ready(function() {
    // all custom jQuery will go here

    $("#nav-placeholder").load("../components/nav.html");

    $("#header-placeholder").load("../components/header.html");

    $("#parameterDropdowns-placeholder").load("../components/parameterDropdowns");

    $(".dropdown-item").on("click", function() {
      console.log("Hi");
      // var value = $(this).val().toLowerCase();
      // console.log(value);
      // $("#myTable tr").filter(function() {
      //   $(this).toggle($(this).text().toLowerCase().indexOf(value) > -1)
      // });
    });

});
