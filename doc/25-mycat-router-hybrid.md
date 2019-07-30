# mycat 2.0 proxy router hybrid

author:junwen 2019-7-29

该路由作为mycat单节点路由的默认实现

io.mycat.command.HybridProxyCommandHandler

是mycat 2.0的路由默认实现,不支持jdbc路由的功能,但是对于schema类型是SQL_PARSE_ROUTE的情况会转发到BlockProxyCommandHandler处理



##### 拦截语句参考

18-proxy-sql.md



##### 路由配置参考

20-mycat-router.md



##### 特点

支持三种schema类型,根据不同的schema类型处理对SQL有限制,SQL只能路由到一个节点

多语句受到路由限制小部分支持

预处理,游标,loaddata取决于命令第一个语句



[![Creative Commons License](https://i.creativecommons.org/l/by-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-sa/4.0/)

This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-sa/4.0/).

------

