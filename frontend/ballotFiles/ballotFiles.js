function ajaxRequest(params) {
    
    var url = 'https://run.mocky.io/v3/855fe37e-0965-4e90-9cb5-1b233c323c07'
    $.get(url + '?' + $.param(params.data)).then(function (res) {
        params.success(res)
    })
}

function filterText() {
    var rex = new RegExp($('#filterText').val());
    if (rex == "/all/") { clearFilter() } else {
        $('.content').hide();
        $('.content').filter(function () {
            return rex.test($(this).text());
        }).show();
    }
}

function clearFilter()
	{
		$('.filterText').val('');
		$('.content').show();
	}