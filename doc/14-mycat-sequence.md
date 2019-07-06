# mycat 2.0 sequenceModifier(SQL修改器,全局序列号sequenceModifier.yml)

author:junwen 2019-6-3.2019-7-6

## SQL修改器中的全局序列号实现

mycat proxy默认功能mysql数据库方式的全局序列号实现,该实现也可以作为mycat proxy 中mysqlapi的使用例子

全局序列号作为路由组件中的sql修改器插件的proxy

整体配置如下

```yaml
modifiers:
  schemaName:
    sequenceModifierClazz: io.mycat.router.sequence.SequenceModifierImpl
    sequenceModifierProperties:
      pattern: (?:(\s*next\s+value\s+for\s*MYCATSEQ_(\w+))(,|\)|\s)*)+
      sequenceHandlerClass: io.mycat.router.sequence.MySQLSequenceHandlerImpl
      mysqlSeqTimeout: 1000
      mysqlSeqSource-ANNOTATION_ROUTE-travelrecord: mytest3306-db1-GLOBAL
      mysqlSeqSource-ANNOTATION_ROUTE-GLOBAL: mytest3306-db1-GLOBAL
```

## SQL修改器的基本设计

## SQL修改器的执行触发点

当前schema的类型是ANNOTATION_ROUTE

SQL修改器的调用点是在SQL被移除schema之后,判断不是需要处理的jdbc语句,进行路由(静态注解)分析之前



##### SQL修改器的属性

sequenceModifierClazz

SQL修改器的包路径

sequenceModifierProperties

SQL修改器的属性,在此属性中的参数会在构造SQL修改器之后把属性作为字符串键值对设置



##### SQL修改器的接口

```java
public interface SequenceModifier {

  void modify(String schema, String sql, ModifyCallback callback);

  void init(MySQLAPIRuntime mySQLAPIRuntime, Map<String, String> properties);
}
```

## SQL修改器的默认实现

io.mycat.router.sequence.SequenceModifierImpl

### 属性

```
sequenceHandlerClass
```

```
pattern
```

pettern是正则表达式,需要实现捕获组

在每次匹配项中

其中能从下标为0的捕获组获取整个符合规则的字符串

下标为1的捕获组序列需要替换的字符串

下标为2的捕获组**序列的名字**,该名字是下面的SequenceHandler.nextId的第二个参数

### 修改器属性作为序列获取器的初始化属性

SequenceModifierImpl会把它接收的属性完整地作为sequenceHandlerClass的属性,也就是说,sequenceHandlerClass的对象构造完成后会接收到与SequenceModifierImpl对象一样的键值对



该类能完成对pattern正则表达式的连续匹配,每一处匹配就执行sequenceHandlerClass获取自增序列并把内容替换



### 全局序列获取器的接口

```java
public interface SequenceHandler {

  void nextId(String schema, String seqName, SequenceCallback callback);

  void init(MySQLAPIRuntime mySQLAPIRuntime, Map<String, String> properties);
}
```



### MySQL数据库全局序列获取器的默认实现

```
io.mycat.router.sequence.MySQLSequenceHandlerImpl
```

```yaml
      mysqlSeqTimeout: 1000
      mysqlSeqSource-schemaName-travelrecord: mytest3306-db1-GLOBAL
      mysqlSeqSource-schemaName-GLOBAL: mytest3306-db1-GLOBAL
```

### 属性

##### mysqlSeqTimeout

向mysql抓取全局序列号任务的超时时间

##### 表与序列源的关联

 mysqlSeqSource-schemaName-travelrecord: mytest3306-db1-GLOBAL

以-为分隔符

 mysqlSeqSource是固定的前缀

schemaName是所在的schema的名字

travelrecord是**序列的名字**,对应上文中捕获组中的值

mytest3306对应replica.yaml中的mysqls中的连接(数据源)配置,该配置只能是proxy内置的mysql协议实现的,也就是非jdbc实现

db1是数据库中全局序列号表所在的schema名字

GLOBAL是全局序列号的名字



综上,一个表的序列信息需要以下信息

1. schema的名字
2. table的名字
3. 连接的名字
4. 全局序列号表所在schema的名字
5. 全局序列号的名字

##### 全局序列号表创建脚本

MySQLSequenceHandlerImpl实现的全局序列号的创建脚本与1.6所用的脚本是相同的,即资源文件夹中的dbseq.sql文件





[![Creative Commons License](https://i.creativecommons.org/l/by-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-sa/4.0/)
This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-sa/4.0/).



------

