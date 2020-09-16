function getCookie(name) {
    var r = document.cookie.match("\\b" + name + "=([^;]*)\\b");
    return r ? r[1] : undefined;
}

function deleteNode(nodeId) {
  xsrf = getCookie("_xsrf");
  $.ajax({
    url:'/admin/nodes/' + nodeId + '/delete',
    method: 'POST',
    headers: {'X-XSRFToken': xsrf},
    success: function () {
      alert('Node deleted.');
    }
  });
}

$(function(){
  $("#table-nodes tr td.td-operations .btnDelete").click(function(){
    var nodeId = $(this).parents('tr').attr('data-node-id');
    deleteNode(nodeId);
  });
});

// $(function(){


// $("#table-nodes tr").mouseover(function(){
//   var nodeId = $(this).attr('data-node-id');
//   console.log(nodeId);
//   var $tdOperations = $(this).children('td.td-operations');
//   $tdOperations.children().remove();
//   var deleteButton = $('<a>').addClass('btn btn-primary').text('Remove');
//   deleteButton.click(deleteNode.bind(this, nodeId));
//   $tdOperations.append(deleteButton);
// }).mouseleave(function(){
//   var nodeId = $(this).attr('data-node-id');
//   console.log('leaving node ' + nodeId);
//   var $tdOperations = $(this).children('td.td-operations');
//   $tdOperations.empty();
// });
//});