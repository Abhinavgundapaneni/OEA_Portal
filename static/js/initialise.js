function get_base_url($this){
    var base_url = (document.URL).slice(0, -1)
    $.ajax({
        type: 'GET',
        url: `${base_url}/home`,
        data : { 'base_url': base_url},
        success : function(json) {
            $("#request-access").hide();
        }
    })
}
get_base_url()