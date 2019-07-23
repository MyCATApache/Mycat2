# mycat 2.0 jdbc

author:junwen 2019-7-22



## jdbc数据源的限制

### 会话同步限制

mycat2 jdbc模块基于jdbc数据源连接池，因为数据源连接池对数据源连接进行了封装，且多数数据源连接池实现不支持数据源连接切换schema（原生的数据源驱动是可以支持的），字符集等操作，所以jdbc模块在简化实现上仅支持切换事务级别和autocommit，开启事务，提交事务，回滚操作。

### 多语句限制

又因为多语句多结果集可以掺杂更新语句，查询语句，加上不同的数据库的驱动实现jdbc驱动有差异，所以mycat jdbc的封装不支持多语句。但是对于一些特定的情况，可以做此优化，后端jdbc会话使用多语句一次性查询多个结果集，mycat可以把这些结果集合拼成一个。

### 非mysql数据源限制

mycat2前端暂时只实现了mysql协议，mysql客户端连接mycat的时候会查询mysql服务器相关的语句，对于jdbc的数据源不是mysql服务器的时候，这些sql只能是mycat本身处理。

### jdbc集群管理

mycat的集群管理需要保证连接是存活可用的，进行心跳，根据数据库服务器的信息判断服务器之间主从同步的信息，来减少读写分离不一致的影响甚至直接把同步延迟太大的服务器直接列为不可用。

一个jdbc连接池已经实现检查存活连接是否存活。它的实现是阻塞的。如果一直阻塞一个连接会影响后续的连接的检查。而阻塞超时时间是连接池的设置的，所以连接池的获取连接的时间要有一个合适的值。

暂时，jdbc心跳配置仅仅支持mysql心跳配置类，其他数据库还没有实现。

### 数据源配置类

jdbc的连接池设置一般来说，不同的连接池有它的设置方法，mycat2没有把连接池的配置做成基于XML配置的Bean方式进行管理，而是采用硬编码适配类作为插件加载。

### Jdbc数据源与Mycat Proxy数据源架构的不同

因为数据源连接池的连接不支持切换schema，而基于mycat proxy自研的mysql协议实现dataNode是可以通过切换schema达到在一个数据源复用一个连接访问不同的schema。所以多个dataNode可以指向一个集群上任意的schema。但是在使用数据源连接池时，多个dataNode只能指向一个集群上一个schema，而集群上每个数据源都是一个数据源连接池。实际上连接池本身会开启额外的线程进行连接的管理，实际上数据源太多可能导致线程数量很多。

### Jdbc数据源与Mycat Proxy数据源主从配置的冲突

mycat proxy自研的连接管理与jdbc数据源连接池是相互独立的，集群管理也是独立的，在主节点点相关的配置上，如果主节点的数据源下标是proxy的数据源（这个配置没有url，就不会被jdbc连接池加载),则jdbc的数据源就会没有主节点。但是数据源是mysql类型的，带有url属性，proxy还是是会加载该数据源的。








[![Creative Commons License](https://i.creativecommons.org/l/by-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-sa/4.0/)
This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-sa/4.0/).

------

