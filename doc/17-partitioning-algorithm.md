# mycat 2.0 分片算法

author:cjw 2019-6-13

[![Creative Commons License](https://i.creativecommons.org/l/by-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-sa/4.0/)
This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-sa/4.0/).

## 分片算法配置

##### defaultNode

一般来说,分片算法都允许配置defaultNode(默认节点),有部分没有实现不支持

当此值为-1时候,表示路由到默认节点报错

0代表第一个节点

##### ranges

分片键值的映射范围,多数情况下,值就是dataNode的下标,0代表第一个节点

键有单值和范围值的两种配置类型,

- 1000 : 0 一对一关系
- 1-2:0 一个连续范围对应一个dataNode1下标

具体看分片算法配置



以下内容引用mycat权威指南的一部分内容

## 非分片

不计算分片值,指定dataNode的下标

```yaml
   function: { clazz: io.mycat.router.function.PartitionConstant , properties: {defaultNode: '0'}} #映射到第一个dataNode
```





## 分片枚举

通过在配置文件中配置可能的枚举 id，自己配置分片，本规则适用于特定的场景，比如有些业务需要按照省
份或区县来做保存，而全国省份区县固定的，这类业务使用本条规则，配置如下： 

```yaml
- clazz: io.mycat.router.function.PartitionByFileMap
  name: PartitionByFileMap
  properties:
    type: Integer
    defaultNode: -1
  ranges:
    10000: 0
    10010: 1
```

##### type

分片字段使用的计算值的类型

Integer,Byte,Char,String,Long,Double,Float,Short,Boolean,BigInteger,BigDecimal

##### defaultNode

##### ranges

单值类型



## 固定hash分片

本条规则类似于十进制的求模运算，区别在于是二进制的操作,是取 id 的二进制低 10 位，即 id 二进制
&1111111111。
此算法的优点在于如果按照 10 进制取模运算，在连续插入 1-10 时候 1-10 会被分到 1-10 个分片，增
大了插入的事务控制难度，而此算法根据二进制则可能会分到连续的分片，减少插入事务事务控制难度。 

```yaml
  - clazz: io.mycat.router.function.PartitionByLong
    name: partitionByLong
    properties:
      partitionCount: 2,1
      partitionLength: 256,512
```

partitionCount 

分片长度是分片范围的一种度量,在此分片算法中,总量是1024

因为分片总长度总量是1024

而且每个dataNode可以占有一个范围的分片长度

所以所有dataNode的分片长度和必须是1024



partitionCount配置一个数组

每个元素是分片节点的数量,所有元素之和就是分片节点数量

每个元素可以对应一个分片长度,在此元素上的每个分片节点使用该长度依次占有分片长度

partitionLength配置一个数组

每个元素是分片长度



partitionCount和partitionLength两个数组长度必须一致

上述例子中

1024 = 2X256+1X512



本例的分区策略：希望将数据水平分成 3 份，前两份各占 25%，第三份占 50%。（故本例非均匀分区）

```
|<———————1024———————————>|

|<—-256—>|<—-256—>|<———-512————->|

| partition0 | partition1 | partition2 | 
```

## 范围约定

```yaml
- clazz: io.mycat.router.function.AutoPartitionByLong
  name: AutoPartitionByLong
  properties:
    defaultNode: -1
  ranges:
    0-500M: 0
    500M-1000M: 1
```

ranges填写多个范围值的映射,可以填写单位K,M

1K=1000

1M=10000

defaultNode 超过范围后的默认节点。
所有的节点配置都是从 0 开始，及 0 代表节点 1，此配置非常简单，即预先制定可能的 id 范围到某个分片

或

```
0-10000000=0
10000001-20000000=1 
```

或



## 取模 

```yaml
- clazz: io.mycat.router.function.PartitionByMod
  name: PartitionByMod
  properties:
    count: '4'
```

count是dataNode的数量

此种配置非常明确即根据 id 进行十进制求模预算，相比固定分片 hash，此种在批量插入时可能存在批量插入单
事务插入多数据分片，增大事务一致性难度。 

## 按日期（天）分片 

```yaml
- clazz: io.mycat.router.function.PartitionByDate
  name: PartitionByDate
  properties:
    dateFormat: yyyy-MM-dd
    beginDate: 2014-01-01
    endDate: 2014-01-02
    parttionDay: 10
```

dateFormat ：日期格式
beginDate ：开始日期
endDate：结束日期
partionDay ：分区天数，即默认从开始日期算起，分隔 10 天一个分区 

如果配置了 endDate 则代表数据达到了这个日期的分片后后循环从开始分片插入 

## 取模范围约束 

```yaml
  - clazz: io.mycat.router.function.PartitionByPattern
    name: PartitionByPattern
    properties:
      patternValue: 256
      defaultNode: -1
    ranges:
      1-32: 0
      33-64: 1
```

patternValue: 即求模基数，

defaoultNode默认节点，如果分片值转换数字失败,则不会按照求模运算,计算结果是默认节点
1-32 即代表 id%256 后分布的范围，如果在 1-32 则在分区 1，其他类推，如果 id 非数据，则
会分配在 defaoultNode 默认节点 

## 截取数字做 hash 求模范围约束 

```yaml
  - clazz: io.mycat.router.function.PartitionByPrefixPattern
    name: PartitionByPrefixPattern
    properties:
      patternValue: 256
      prefixLength: 5
    ranges:
      1-4: 0
      5-8: 1
      9-12: 2
      13-16: 3
      17-20: 4
      21-24: 5
      25-28: 6
      29-32: 7
      0-0: 7
```

patternValue:即求模基数

prefixLength:ASCII 截取的位数 

1-32 即代表 分片值%256 后分布的范围，如果在 1-32 则在分区 1，其他类推 

采取的是将列种获取前 prefixLength 位列所有 ASCII 码的和进行求模
sum%patternValue ,获取的值，在范围内的分片数， 

## 应用指定

```yaml
- clazz: io.mycat.router.function.PartitionDirectBySubString
  name: PartitionDirectBySubString
  properties:
    startIndex: 0
    size: 2
    partitionCount: 8
    defaultNode: 0
```

此规则是在运行阶段有应用自主决定路由到那个分片。 

此方法为直接根据字符子串（必须是数字）计算分区号（由应用传递参数，显式指定分区号）。
例如 id=05-100000002
在此配置中代表根据 id 中从 startIndex=0，开始，截取 size=2 位数字即 05，05 就是获取的分片，
默认分配到 defaultNode

## 截取数字 hash 解析(sharding-string-hash) 

```yaml
- clazz: io.mycat.router.function.PartitionByString
  name: PartitionByString
  properties:
    partitionLength: 128,128,256
    partitionCount: 2,2,2
    hashSlice: -1:0
```

hashSlice : 配置 分片键值 范围截取规则； （hash 预算位 格式为 start:end）
partitionLength 每个分区占用长度
partitionCount 分区数量
hashSlice 示例： 假设 user_id = 11356789,以下规则截取出的结果如下所示
配置中的特殊值（0 means str.length(), -1 means str.length()-1） 

```
“2” -> (0,2) -> 1
“1:2” -> (1,2) -> 11
“1:” -> (1,0) -> 1356789
“-1:” -> (-1,0) -> 9
“:-1” -> (0,-1) -> 1135678
“:” -> (0,0) -> 11356789 
```

分区范围： 需要 partitionLength 和 partitionCount 配合使用 

```
partitionLength : 128,128,256
partitionCount : 2,2,2 
```

所有分区范围必须是 0~1024： 如上所示 128*2 + 128*2 +256 *2 = 1024 (sum(partitionLength[i]*partitionCount[i]))
如上配置表示： 总共有 6 个分区,每个分区范围如下所示 

```
1， 2， 3， 4， 5， 6

0~128,128~256,256-384,384-512,512-768,768-1024 
```



## 一致性 hash

一致性 hash 预算有效解决了分布式数据的扩容问题

```yaml
- clazz: io.mycat.router.function.PartitionByMurmurHash
  name: PartitionByMurmurHash
  properties:
    seed: 0
    count: 2
    virtualBucketTimes: 160
  ranges:
    1: 0 # 键:权重
```

seed :默认是 0

count:要分片的数据库节点数量， 必须指定， 否则没法分片 

virtualBucketTimes :一个实际的数据库节点被映射为这么多虚拟
节点， 默认是 160 倍， 也就是虚拟节点数是物理节点数的 160 倍 

ranges:

节点的权重， 没有指定权重的节点默认是 1。 以 properties 文件的格式填写， 以从 0 开始到 count-1 的整数值也就是节点索引为 key， 以节点权重值为值。 所有权重值必须是正整数， 否则以 1 代替 

## 按单月小时拆分

此规则是单月内按照小时拆分，最小粒度是小时，可以一天最多 24 个分片，最少 1 个分片，一个月完后下月
从头开始循环。
每个月月尾，需要手工清理数据。 

```yaml
- clazz: io.mycat.router.function.PartitionByLatestMonth
  name: PartitionByLatestMonth
  properties:
    dateFormat: yyyymmddHH
    splitOneDay: 24
```

splitOneDay ： 一天切分的分片数 

## 范围求模分片

先进行范围分片计算出分片组，组内再求模
优点可以避免扩容时的数据迁移，又可以一定程度上避免范围分片的热点问题
综合了范围分片和求模分片的优点，分片组内使用求模可以保证组内数据比较均匀，分片组之间是范围分片可以兼顾范围查询。
最好事先规划好分片的数量，数据扩容时按分片组扩容，则原有分片组的数据不需要迁移。由于分片组内数据比较均匀，所以分片组内可以避免热点数据问题。 

```yaml
- clazz: io.mycat.router.function.PartitionByRangeMod
  name: PartitionByRangeMod
  properties:
    defaultNode: -1
  ranges:
    0-200M: 5
    200M-400M: 1
    400M-600M: 4
    600M-800M: 4
    800M1-1000M: 6
```

## 日期范围 hash 分片

思想与范围求模一致，当由于日期在取模会有数据集中问题，所以改成 hash 方法。
先根据日期分组，再根据时间 hash 使得短期内数据分布的更均匀
优点可以避免扩容时的数据迁移，又可以一定程度上避免范围分片的热点问题
要求日期格式尽量精确些，不然达不到局部均匀的目的 

```yaml
- clazz: io.mycat.router.function.PartitionByRangeDateHash
  name: PartitionByRangeDateHash
  properties:
    dateFormat: yyyy-MM-dd HH:mm:ss
    partionDay: 3
    groupPartionSize: 6
```

partionDay 代表多少天分一个分片
groupPartionSize 代表分片组的大小 

## 冷热数据分片

根据日期查询日志数据 冷热数据分布 ，最近 n 个月的到实时交易库查询，超过 n 个月的按照 m 天分片。 

```yaml
- clazz: io.mycat.router.function.PartitionByHotDate
  name: PartitionByHotDate
  properties:
    dateFormat: yyyy-MM-dd
    lastDay: 10
    partionDay: 30
```

## 自然月分片

```yaml
- clazz: io.mycat.router.function.PartitionByMonth
  name: PartitionByMonth
  properties:
    formatter: yyyy-MM-dd
    beginDate: 2014-01-01
    endDate: 2014-12-01
```

columns： 分片字段，字符串类型
dateFormat ： 日期字符串格式,默认为 yyyy-MM-dd
beginDate ： 开始日期，无默认值
endDate：结束日期，无默认值
节点从 0 开始分片 

------

### 使用场景：

#### 场景 1：

默认设置； 节点数量必须是 12 个， 从 1 月~12 月

```
 "2014-01-01" = 节点 0
"2013-01-01" = 节点 0
 "2018-05-01" = 节点 4
 "2019-12-01" = 节点 11
```



#### 场景 2：

beginDate = "2017-01-01"
该配置表示"2017-01 月"是第 0 个节点， 从该时间按月递增， 无最大节点

```
"2014-01-01" = 未找到节点
"2017-01-01" = 节点 0
 "2017-12-01" = 节点 11
 "2018-01-01" = 节点 12
 "2018-12-01" = 节点 23
```



#### 场景 3：

beginDate = "2015-01-01"sEndDate = "2015-12-01"
该配置可看成与场景 1 一致； 场景 1 的配置效率更高

```
"2014-01-01" = 节点 0
 "2014-02-01" = 节点 1
 "2015-02-01" = 节点 1
 "2017-01-01" = 节点 0
 "2017-12-01" = 节点 11
 "2018-12-01" = 节点 11
```

该配置可看成是与场景 1 一致

该配置可看成是与场景 1 一致

#### 场景 4：

beginDate = "2015-01-01"sEndDate = "2015-03-01"
该配置标识只有 3 个节点； 很难与月份对应上； 平均分散到 3 个节点上 

------

