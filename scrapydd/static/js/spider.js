function getCookie(name) {
    var r = document.cookie.match("\\b" + name + "=([^;]*)\\b");
    return r ? r[1] : undefined;
}


$(function(){
    $("table#joblist tr td.job_id").each(function(i,e){
        var project_name = $('#projectName').val();
        var spider_name = $('#spiderName').val();
        var job_id = $(e).text();
        var delete_button = $("<a href='#'>").text('Delete').click(function(){
            xsrf = getCookie("_xsrf");
            $.ajax({
                url: '/projects/' + project_name + '/spiders/' + spider_name + '/jobs/' + job_id + '/delete',
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
        var project = $('#projectName').val();
        var spider = $('#spiderName').val();
        xsrf = getCookie("_xsrf");
        $.ajax({
            url: '/projects/' + project + '/spiders/' + spider + '/run',
            method: "POST",
            headers: {'X-XSRFToken': xsrf},
            data: {'project': project, 'spider': spider},
            success: function(){
                alert('Spider started.');
            },
            error: function(jqXHR, textStatus, errorThrown){
                alert(textStatus + ':' + errorThrown);
            }
        });
    })
});