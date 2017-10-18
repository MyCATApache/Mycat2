# tcp-proxy
Mycat 2.0 预览版。

基于Nio实现，有效管理线程，解决高并发问题。

前后端共享buffer，支持全透传和半透传，极致提升内核性能，稳定性和兼容性。





# 功能特性

- [x] 支持SQL92标准。

- [x] 支持单库内任意sql。

- [x] 支持读写分离。

- [x] 自动集群管理。

- [x] 支持负载均衡。

- [x] 支持主从切换。

- [x] 支持动态注解。

- [x] 结果集缓存。





# 配置说明

- mycat.yml：mycat代理的配置，指定开启的端口号提供代理服务

- user.yml: 配置mycat的用户名密码和白名单

- cluster.yml：集群配置，可以开启关闭集群功能，指定集群端口和id号，id在集群内必须唯一

- balancer.yml：负载均衡配置，可以开启关闭负载均衡，负载均衡只有在集群模式下才生效

- heartbeat.yml：心跳配置，指定心跳周期及切换间隔

- schema.yml：mycat的逻辑库

- datasource.yml：后端数据库的复制组配置

- replica-index.yml：指定datasource.yml中复制组的写节点，默认为0

- sharding-rule.yml：分片规则





# 启动运行


## 一、本地调试

配置文件只能有一个，在IDEA中调试可以设置启动参数，启动参数优先级比配置文件高，会替换掉配置文件的参数，现支持的启动参数：

- -mycat.proxy.port 8067

- -mycat.cluster.enable true

- -mycat.cluster.port 9067

- -mycat.cluster.myNodeId leader-2





## 二、编译运行

### 1. 下载源码并编译

1. clone源代码 git clone https://github.com/MyCATApache/tcp-proxy.git

2. maven编译 mvn clean install

3. 在target目录下找到操作系统对应的压缩包，如linux下的mycat2-0.1-20170906223147-linux.tar.gz

4. 将压缩包解压缩到指定路径





### 2. 修改配置文件

配置文件在conf目录下，需要修改的配置文件包括：

1. mycat.yml，指定ip和端口号

2. user.yml，配置user信息，包括name和password，登录的时候需要按照指定的用户名密码登录，schemas对应为schema.yml中的schema，白名单功能默认关闭

3. cluster.yml，指定是否开启集群模式以及集群节点的基本信息，默认集群关闭

4. balancer.yml，指定是否开启负载均衡模式以及负载均衡的基本信息，默认负载均衡关闭

5. heartbeat.yml，配置心跳相关信息，可以使用默认值

6. schema.yml，设置相关的schema

7. datasource.yml，设置后端连接的复制组信息

8. replica-index.yml，设置复制组写节点配置，默认为0





### 3. 运行

1. 运行的方式与 1.6 相同

2. linux 环境 运行 bin 目录下 ./mycat {console | start | stop | restart | status | dump }

3. window 环境 运行 bin 目录下 startup_nowrap.bat

4. 运行成功后，使用 root 账号登录，登录密码 123456 登录，端口号为mycat.yml中配置的端口号，默认为8066





### 4. 集群启动

1. conf目录下，需要正确配置mycat.yml，cluster.yml，balancer.yml

2. 配置完成后，按照第三步的方式依次启动各个节点的mycat，将自动进行集群管理
