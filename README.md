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

### 运行方式：

#### 一 下载

   1.1  window 环境 下载 mycat2-0.1-20170906223147-win.tar.gz <br>
   1.2  linux 环境 下载 mycat2-0.1-20170906223147-linux.tar.gz<br>

#### 二 修改配置文件

   2.1  需要修改 conf 目录下 schema.xml datasource.xml 两个配置文件。<br>
   2.2  schema.xml 中 需要设置 default-db (默认数据库), default-rep (默认复制组) 属性。<br>
   2.3  datasource.xml 中 需要设置 ip port user password min-con max-con 属性。<br>

#### 三 运行

   3.1  运行的方式与 1.6 相同。<br>
   3.2  linux 环境 运行 bin 目录下 ./mycat {console | start | stop | restart | status | dump }<br>
   3.3  window 环境 运行 bin 目录下 startup_nowrap.bat<br>
   3.4  运行成功后， 使用 root 账号登录，登录密码 123456 登录， 端口号 8066<br>

#### 四 启动第二个mycat，并自动加入集群。

   4.1  集群相关配置文件
        `conf` 目录下, mycat1.conf,mycat2.conf,mycat3.conf 三个配置文件，分别为集群中三个节点的配置文件。<br>
        配置文件名称格式为: `mycat[unique id]. conf`<br>
        配置文件中 `cluster.allnodes` 属性 需要将集群中，所有节点的信息配置上。<br>

        `conf` 目录下, 设置 `wrapper.conf` 配置文件中,<br>
        `wrapper.app.parameter.2` 参数的值 为  mycat[`unique id`]. conf 中的  `unique id` 。<br>
        确定从哪个mycat[unique id]. conf 配置文件中 读取配置。<br>
   4.2  需要注意的是，每个节点一套mycat程序。<br>
   4.3  配置完成后 按照第三步 启动mycat. 新启动的mycat 将自动加入集群中。<br>
