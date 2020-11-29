# Mycat2

![](https://github.com/MyCATApache/Mycat2/workflows/Java%20CI%20-%20Mycat2%20Main/badge.svg)
![](https://github.com/MyCATApache/Mycat2/workflows/Java%20CI%20-%20Mycat2%20Dev/badge.svg)

该readme显示的版本尚未发布,是master仓库,已经发布的旧版本(配置完全不一样)在release里

[文档](https://github.com/MyCATApache/Mycat2/wiki)



### 入门Mycat2



![product_arch_single](https://raw.githubusercontent.com/wiki/MyCATApache/Mycat2/img/product_arch_single.png)







右侧的MySQL在Mycat2的配置里面叫做prototype，专门用于响应兼容性SQL和系统表SQL。



###### 什么是兼容性SQL？

客户端或者所在应用框架运行所需必须的SQL，但是用户一般接触不到，它们会影响客户端的启动，运行。而Mycat对于这种SQL尽可能不影响用户使用。



###### 什么prototype服务器?

分库分表中间件中用于处理MySQL的兼容性SQL和系统表SQL的服务器,这个配置项可以指向一个服务器也可以是一个集群,Mycat依赖它处理非select,insert,update,delete语句。当这个服务器是与第一个存储节点是同一个服务器/集群的时候,人们一般叫它做0号节点。



![product_arch_expand](https://raw.githubusercontent.com/wiki/MyCATApache/Mycat2/img/product_arch_expand.png)

当需要进行数据分片的时候，通过扩展存储节点。



##### 入门

前提:

准备两个MySQL服务器(prototype服务器)，端口`3306`,`3307` ，用户名：`root` 密码：`123456`

Mycat2的jar包



##### Mycat服务器级别配置

保证配置文件夹有server.json，内容至少是

```sql
{
  "server":{
    "ip":"127.0.0.1",
    "mycatId":1,
    "port":8066,
  }
}
```



[启动请参考文档(安装与启动)](https://github.com/MyCATApache/Mycat2/wiki)



启动过程中,Mycat会在配置文件夹生成默认配置,加载上述的MySQL中的系统表,并建立用户名为

`root` 密码为`123456`



此时使用客户端登录Mycat即可



Mycat可以在控制台创建库

```sql
CREATE DATABASE db1
```

建库语句执行两个操作

1.更改mycat的整个库配置

2.在prototype服务器执行此SQL



Mycat可以在控制台操作



###### 切换逻辑库

```sql
USE `db1`;
```

建表语句执行两个操作

1.更改mycat的整个库配置

2.在prototype服务器执行此SQL



###### 删除逻辑库

```sql
DROP DATABASE db1
```

建表语句执行两个操作

1.更改mycat的整个库配置

2.在prototype服务器执行此SQL



###### 创建单表

```sql
CREATE TABLE db1.`travelrecord` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `user_id` varchar(100) DEFAULT NULL,
  `traveldate` date DEFAULT NULL,
  `fee` decimal(10,0) DEFAULT NULL,
  `days` int DEFAULT NULL,
  `blob` longblob,
  PRIMARY KEY (`id`),
  KEY `id` (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8;
```



建表语句执行两个操作

1.更改mycat的整个库配置

2.在prototype服务器执行此SQL



###### 使用注释动态配置Mycat

如果使用MySQL官方客户端添加 **`-c`** 参数避免客户端过滤注释



###### 添加存储数据源

```json
/*+ mycat:createDataSource{
"name":"dw0",
"url":"jdbc:mysql://127.0.0.1:3306",
"user":"root",
"password":"123456"
} */;

/*+ mycat:createDataSource{
"name":"dr0",
"url":"jdbc:mysql://127.0.0.1:3306",
"user":"root",
"password":"123456"
} */;

/*+ mycat:createDataSource{
"name":"dw1",
"url":"jdbc:mysql://127.0.0.1:3307",
"user":"root",
"password":"123456"
} */;

/*+ mycat:createDataSource{
"name":"dr1",
"url":"jdbc:mysql://127.0.0.1:3307",
"user":"root",
"password":"123456"
} */;
```



###### 添加集群配置

```json
/*! mycat:createCluster{"name":"c0","masters":["dw0"],"replicas":["dr0"]} */;

/*! mycat:createCluster{"name":"c1","masters":["dw1"],"replicas":["dr1"]} */;
```



###### 删除表

```sql
drop table db1.travelrecord
```

删表语句执行两个操作

1.更改mycat的整个库配置

2.在prototype服务器执行此SQL,而不会在其他存储节点执行



###### 创建全局表

```sql
CREATE TABLE db1.`travelrecord` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `user_id` varchar(100) DEFAULT NULL,
  `traveldate` date DEFAULT NULL,
  `fee` decimal(10,0) DEFAULT NULL,
  `days` int DEFAULT NULL,
  `blob` longblob,
  PRIMARY KEY (`id`),
  KEY `id` (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8 BROADCAST;
```

建全局表语句执行操作

1.更改mycat的整个库配置

2.在prototype服务器执行此SQL

3.根据当前集群名字首字母为c的集群纳入到全局表的存储节点中

4.根据存储节点信息建立物理库,物理表



###### 创建分片表

```sql
CREATE TABLE db1.`travelrecord` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `user_id` varchar(100) DEFAULT NULL,
  `traveldate` date DEFAULT NULL,
  `fee` decimal(10,0) DEFAULT NULL,
  `days` int DEFAULT NULL,
  `blob` longblob,
  PRIMARY KEY (`id`),
  KEY `id` (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8 dbpartition by hash(id) tbpartition by hash(user_id) tbpartitions 2 dbpartitions 2;
```

建分片表语句执行操作

1.更改mycat的整个库配置

2.在prototype服务器执行此SQL

3.根据当前集群名字首字母为c的集群纳入到分片表的存储节点中

4.根据存储节点信息建立物理库,物理表



默认分片表的自增序列是雪花算法



## 核心团队

[junwen12221](https://github.com/junwen12221)

[wangzihaogithub](https://github.com/wangzihaogithub)

[zwyqz](https://github.com/zwyqz)



## License

GNU GENERAL PUBLIC LICENSE