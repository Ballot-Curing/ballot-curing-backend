$(document).ready(function() {
    // all custom jQuery will go here

    $("#nav-placeholder").load("../components/nav.html");

    $("#header-placeholder").load("../components/header.html");

    $("#issueDropdown").click(function() {
      var value = $(this).val().toLowerCase();
      $("#myTable tr").filter(function() {
        $(this).toggle($(this).text().toLowerCase().indexOf(value) > -1)
      });
    });

});
