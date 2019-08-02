$(function(){
    $("table#joblist tr td.job_id").each(function(i,e){
        var project_name = $('#project_name').val();
        var spider_name = $('#spider_name').val();
        var job_id = $(e).text();
        var delete_button = $("<a href='#'>").text('Delete').click(function(){
            $.ajax({
                url: '/projects/' + project_name + '/spiders/' + spider_name + '/jobs/' + job_id + '/delete',
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
});