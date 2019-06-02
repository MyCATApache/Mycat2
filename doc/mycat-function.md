## mycat 2.0-function(function.yml,分片算法)

author:junwen 2019-6-1

[![Creative Commons License](https://i.creativecommons.org/l/by-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-sa/4.0/)
This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-sa/4.0/).

## 概念

### 分片算法

分片算法是根据分片键计算出分片范围的算法

一个分片节点有一个连续数值范围

多个分片节点有多个连续数值范围

分片算法就是一个值映射到一个分片节点的算法

更通用地,它应该支持连续的值映射到(0个或者多个)分片节点

### 无状态分片算法

分片算法与分片表的状态无关,称之为无状态分片算法.这类算法的配置一般是人为设置改变的.不具备自动化功能.

### 有状态分片算法

分片算法与分片表的状态有关,称之为有状态分片算法.这类算法会因为mycat一些自动化功能导致分片算法的参数变更,例如分区数变更.所以该分片算法的状态与分片表的状态是对应的,耦合的,

## 前提

mycat提供的分片算法一般放置在router模块(io.mycat.router.function),也可以在plug模块里面提供

mycat 2.0暂时还没提供有状态分片算法的具体例子,但是可以参考1.6的有状态分片算法章子了解

## 配置

```yaml
funtions:
  - clazz: io.mycat.router.function.PartitionByLong
    name: partitionByLong
    properties:
      partitionCount: '4'
      partitionLength: '256'

```

clazz

分片算法类路径

name

分片算法的名字,供外部引用

properties

分片算法的参数与值,在分片算法初始化时候作为初始化参数



