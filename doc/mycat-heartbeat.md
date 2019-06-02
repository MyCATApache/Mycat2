## mycat 2.0-heart(heart.yml,心跳)

author:junwen,zhangwy 2019-6-2

[![Creative Commons License](https://i.creativecommons.org/l/by-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-sa/4.0/)
This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-sa/4.0/).

## 概念

## 心跳(heartbeat)

mycat启动定时器,对mysql数据源发送心跳,用于检测mysql数据源是否可用.如果mysql数据源为master节点,则会根据replica的switchType类型来决定是否进行主从的切换.


## 集群心跳周期(replicaHeartbeatPeriod)

用于配置心跳定时器的定时检测的时间周期.

单位为秒.

## 最小心跳检测间隔(minHeartbeatChecktime)

如果上一个心跳已经正常返回,则正常的发送心跳.

如果上一个心跳发送还未返回,而且间隔小于minHeartbeatChecktime,则不再发新的心跳,间隔已经大于mineartbeatChecktime,则说明上一个心跳已经超时,不接受上一个心跳的结果,并设置为mysql源为超时.

单位为毫秒.

## 错误重试次数(maxRetry)

当心跳返回为错误,或者超时时候,需要连续的3次的错误才能设置为错误,避免因为网络抖动,导致数据源的变成异常状态.

## 最小心跳切换时间(minSwitchTimeInterval)

上一次主从的切换与当前需要切换的时间的间隔必须要超过minSwitchTimeInterval,才允许发生切换.

```yaml
heartbeat:
  replicaHeartbeatPeriod: 120 #集群心跳周期 秒         心跳定时器的时间.
  minHeartbeatChecktime: 12000 #最小心跳间隔 毫秒      超过这个阈值的间隔的时间 判定为心跳超时,如果上一个心跳还未返回而且小于这个阈值不发送心跳继续等待心跳结果.
  maxRetry: 3 #错误重试次数
  minSwitchTimeInterval: 12000 # 毫秒  主从心跳最小切换时间
  
```