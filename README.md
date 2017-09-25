# tcp-proxy
Genel TCP Proxy using Java NIO ,simple and fast

学习Mycat核心，学习NIO编程，都可以从这个代码来理解。可以用来实现如下目标：
高性能HTTP Proxy
高性能FTP Proxy
高性能WebService 网关

如果代码看不懂，建议去看看《架构解密》中关于NIO的内容，或者报名Leader us私塾 中间件研发课程，QQ 群 106088787 




## 目前已实现的特性：

*  基于Nio实现，有效管理线程，解决高并发问题。
*  自动集群功能。
*  支持SQL92标准。
*  支持单库内任意sql。
*  前后端共享buffer,支持全透传和半透传，极致提升内核性能，稳定性 和 兼容性。



### 集群配置说明

- 非集群模式
  - 指定`mycat.yml`中的ip和port
  - 修改`cluster.yml`中的`enable: false`，设置cluster的port


- 集群模式
  - 开启集群：`cluster.yml`中的`enable: true`，设置cluster的port，`myNodeId`需要在集群中唯一
  - 负载均衡：`balancer.yml`中的`enable: true`，设置balancer的port及strategy
  - 集群模式下，只有在集群状态下才提供代理服务，在脱离集群状态下将暂时无法提供代理服务



### 运行方式：

#### 一 下载

   1.1  window 环境 下载 mycat2-0.1-20170906223147-win.tar.gz <br>
   1.2  linux 环境 下载 mycat2-0.1-20170906223147-linux.tar.gz<br>

#### 二 修改配置文件

   2.1  需要修改 conf 目录下 `schema.yml` `datasource.yml` 两个配置文件。<br>
   2.2  `schema.yml`中 需要设置 default-db (默认数据库), default-rep (默认复制组) 属性。<br>
   2.3  `datasource.yml` 中 需要设置 ip port user password min-con max-con 属性。<br>

#### 三 运行

   3.1  运行的方式与 1.6 相同。<br>
   3.2  linux 环境 运行 bin 目录下 ./mycat {console | start | stop | restart | status | dump }<br>
   3.3  window 环境 运行 bin 目录下 startup_nowrap.bat<br>
   3.4  运行成功后， 使用 root 账号登录，登录密码 123456 登录， 端口号 8066<br>

#### 四 启动第二个mycat，并自动加入集群。

   4.1  集群相关配置文件
        `conf` 目录下, 修改mycat.yml,cluster.yml,balancer.yml 三个配置文件。<br>
        配置文件中 `cluster.allnodes` 属性 需要将集群中，所有节点的信息配置上。<br>
   4.2  需要注意的是，每个节点一套mycat程序。<br>
   4.3  配置完成后 按照第三步 启动mycat. 新启动的mycat 将自动加入集群中。<br>

### 五 在IDEA中调试集群

    IDEA中调试可以设置启动参数，支持的启动参数：
        -mycat.proxy.port 8067
        -mycat.cluster.enable true
        -mycat.cluster.port 9067
        -mycat.cluster.myNodeId leader-2