# mycat 2.0 负载均衡算法

author:zwy 2019-6-13

[![Creative Commons License](https://i.creativecommons.org/l/by-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-sa/4.0/)
This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-sa/4.0/).


##负载均衡算法

对于一条查询语句，可以从几个mysql数据源进行选取，选取的时候会根据不同的策略进行选取。

##BalanceLeastActive

最少正在使用的连接数的mysql数据源被选中，如果连接数相同,则从连接数相同的数据源中的随机，使慢的机器收到更少。


##BalanceRandom

利用随机算法产生随机数，然后从活跃的mysql数据源中进行选取。

##BalanceRoundRobin

加权轮训算法，记录轮训的权值，每次访问加一，得到n，然后对mysql数据源进行轮训，如果权值已经为零，则跳过，如果非零则减一，n减1，直n为零则选中的节点就是需要访问的mysql数据源节点。

##BalanceRunOnReplica

io.mycat.plug.loadBalance.BalanceRunOnReplica

把请求尽量发往从节点,不会把请求发到不可读(根据延迟值判断)与不可用的从节点















------

