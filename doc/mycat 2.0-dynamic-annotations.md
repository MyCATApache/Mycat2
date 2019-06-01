# mycat 2.0-dynamic-annotations(动态注解)

author:junwen 2019-6-1

[![Creative Commons License](https://i.creativecommons.org/l/by-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-sa/4.0/)
This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-sa/4.0/).

## 前提

- 静态注解可能会覆盖动态注解的结果
- 设置schema的类型为ANNOTATION_ROUTE,则该schema开启动态注解
- 动态注解依赖schema和function(分片算法)的配置

## 动态注解配置(rule)

### 不使用动态注解的分片规则配置(暂时不支持)

```yaml
tableRules:
  - funtion: partitionByLong
    name: travelrecord
    rules:
      - column: id

```

在此配置下,使用SQL解析可以分析出字段的查询类型与值,所以不需要使用动态注解提取

### 使用动态注解的分片

```yaml
tableRules:
  - funtion: partitionByLong
    name: travelrecord
    rules:
      - column: id
        equalAnnotations:
          #  SELECT * FROM `travelrecord` WHERE id = 1;
          #  DELETE FROM `travelrecord` WHERE id = 2
          #  INSERT INTO `travelrecord` (`id`,`user_id`) VALUES (3,2);
          - id = (?<id1>([0-9]*))
          - VALUES \((?<id2>([0-9]*))
        equalKey: id1,id2
        rangeAnnotations:
          # SELECT * FROM `travelrecord` WHERE id between 1 and 128;分片算法分区大小,跨分片不能查询
          - '((id (?:BETWEEN )(?<id1s>([0-9]*))(?: AND )(?<id1e>([0-9]*))))'
        rangeStartKey: id1s
        rangeEndKey: id1e
```

#### tableRules

配置多个tableRule,其中name属性在配置中唯一并与schema中的table对应

#### function

配置分片算法的名字,该名字必须与分片算法里面的配置对应

#### rules

配置规则

#### column

分片字段(键)

#### equalAnnotations

提取分片值的正则表达式(使用捕获组获得值) 

可以配置多个多个正则表达式,但是只能有一个值生效

#### equalKey

equalAnnotations的正则表达式对应的捕获组的名字,以,分隔可以配置多个值,但是只能有一个值生效

#### rangeAnnotations

提取范围查询分片值的正则表达式(使用捕获组获值,需要捕获两个) 

可以配置多个多个正则表达式

#### rangeStartKey

rangeAnnotations的正则表达式对应的捕获组的名字,以,分隔可以配置多个值,但是只能有一个值生效

#### rangeEndKey

rangeAnnotations的正则表达式对应的捕获组的名字,以,分隔可以配置多个值,但是只能有一个值生效



一般情况下,equalKey与rangeStartKey,rangeEndKey必须一个生效,而且只有一个,但是他们都生效的时候,如果计算得出的DataNode是相同的,则不会抛出异常