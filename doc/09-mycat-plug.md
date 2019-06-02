# mycat 2.0-plug(插件)

author:junwen 2019-6-1

<a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-sa/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/">Creative Commons Attribution-ShareAlike 4.0 International License</a>.



## 前提

插件模块被proxy依赖proxy



## 负载均衡算法配置

```yaml
defaultLoadBalance: BalanceRandom
loadBalances:
  - name: BalanceRandom
    clazz: io.mycat.plug.loadBalance.BalanceRandom
```

defaultLoadBalance

当指定的负载均衡算法不存在的时候使用该算法

loadBalances

配置多个负载均衡算法

name

负载均衡算法的名字,供外部引用

clazz

类路径

