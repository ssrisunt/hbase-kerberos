 <script type="text/javascript">
            $(document).ready(function () {
                $('button.btn-primary').click(function(){
                    $('#formView').hide();
                    $('#treeView').show();
                    var tree = $.parseXML($('textarea').val());
                    traverse($('#treeView li'),tree.firstChild)
                    // this – is an —
                    $('<b>–<\/b>').prependTo('#treeView li:has(li)').click(function(){
                        var sign=$(this).text()
                        if (sign=="–")
                            $(this).text('+').next().children().hide()
                        else
                            $(this).text('–').next().children().show()
                    });
                });
                
                function traverse(node,tree) {
                    var children=$(tree).children()
                    node.append(tree.nodeName)
                    if (children.length){
                        var ul=$("<ul>").appendTo(node)
                        children.each(function(){
                            var li=$('<li>').appendTo(ul)
                            traverse(li,this)
                        })
                    }else{
                        $('<ul><li>'+ $(tree).text()+'<\/li><\/ul>').appendTo(node)
                    }
                }
            });
    </script>
