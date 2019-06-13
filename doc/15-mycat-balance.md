# mycat 2.0 balance(负载均衡)

author:junwen 2019-6-3

[![Creative Commons License](https://i.creativecommons.org/l/by-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-sa/4.0/)
This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-sa/4.0/).



 负载均衡的源于一个副本中从多个数据源中选择一个的需求,它与分片算法最重要的不同点在于它没有一个严格的一对一的映射关系,允许不确定的选择规则.

实际上这个负载均衡并不限于副本中,例如全局表的读查询选择,它在多个节点中选择一个节点来查询.



## 负载均衡元素

一般来说,负载均衡的元素可以没有属性,而它的属性取决于具体的算法

可能的属性

1. 元素名字
2. 主从
3. 负载数量
4. 人为定义权重

## 负载均衡切面

选择reactor(没实现)

选择dataNode(计划中)

选择dataSource(已经实现)



## 负载均衡插件配置

```yaml
defaultLoadBalance: BalanceRandom
loadBalances:
  - name: BalanceRandom
    clazz: io.mycat.plug.loadBalance.BalanceRandom
```

defaultLoadBalance

默认的负载均衡算法的名字

loadBalances

多个负载均衡算法







