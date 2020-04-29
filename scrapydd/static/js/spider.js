function getCookie(name) {
    var r = document.cookie.match("\\b" + name + "=([^;]*)\\b");
    return r ? r[1] : undefined;
}


$(function(){
    $("table#joblist tr td.job_id").each(function(i,e){
        var project_id = $('#projectId').val();
        var spider_id = $('#spiderId').val();
        var job_id = $(e).text();
        var delete_button = $("<a href='#'>").text('Delete').click(function(){
            xsrf = getCookie("_xsrf");
            $.ajax({
                url: '/projects/' + project_id + '/spiders/' + spider_id + '/jobs/' + job_id + '/delete',
                headers: {'X-XSRFToken': xsrf},
                method: "POST",
                success: function(){
                    location.reload();
                }
            });
        });
        $(e).mouseenter(function(){
            $(e).append(delete_button);
            delete_button.show();
            //alert(job_id);
        }).mouseleave(function(){
            delete_button.hide();
        });
    });

    $("#btnRun").click(function(){
        var project_id = $('#projectId').val();
        var spider_id = $('#spiderId').val();
        xsrf = getCookie("_xsrf");
        $.ajax({
            url: '/projects/' + project_id + '/spiders/' + spider_id + '/run',
            method: "POST",
            headers: {'X-XSRFToken': xsrf},
            success: function(){
                alert('Spider started.');
            },
            error: function(jqXHR, textStatus, errorThrown){
                alert(textStatus + ':' + errorThrown);
            }
        });
    })
});