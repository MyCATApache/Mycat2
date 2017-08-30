https://github.com/kkzfl22/tcp-proxy
https://github.com/yanjunli/tcp-proxy
https://github.com/ynfeng/tcp-proxy/
NIO 学习 http://ifeve.com/selectors/
一种以ID特征为依据的数据分片（Sharding）策略
http://www.cnblogs.com/JeffreyZhao/archive/2010/03/09/sharding-by-id-characteristic.html
github上fork了别人的项目后，再同步更新别人的提交 
http://jinlong.github.io/2015/10/12/syncing-a-fork/
http://blog.csdn.net/qq1332479771/article/details/56087333
mysql 协议
https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_packets.html

1. 会话相关
session : 代表一个会话
AdminSession Mycat各个节点发起会话,规定Node name大的节点主动向Node Name节点小的发起连接请求 

AbstractSession  会话，代表一个前端连接
AbstractMySQLSession 抽象的MySQL的连接会话
MycatSession 前端连接会话

MycatSessionManager 用来处理新的连接请求并创建Session
DefaultAdminSessionManager 默认的管理会话

2. 会话处理器
NIOHandler  此NIO Handler应该是单例的，能为多个Session会话服务
MySQLClientAuthHandler MySQL客户端登录认证的Handler，为第一个Handler
DefaultAdminSessionHandler  默认的负责处理AdminSession的命令
AbstractBackendIOTask  默认抽象类
BackendConCreateTask  创建后端MySQL连接并负责完成登录认证的Processor

MySQLPacket mysql数据报
AuthPacket mysql用户认证数据报

管理报文，3个字节的包头 ，其中前两个字节为包长度，第3个字节为包类型，最后为报文内容。
ManagePacket 
节点信息的报文，用于向对方表明自己的身份信息以及自己所处的集群状态
NodeRegInfoPacket 

SQLCommand 负责处理SQL命令
DirectPassthrouhCmd  直接透传命令报文

3. 命令管理
AdminCommandResovler 返回管理报文对应的处理命令
AdminCommand  负责解析请求的命令报文并且正确应答报文

4.集群相关
MyCluster Mycat集群，保存了当前节点成员以及状态
ClusterState Mycat集群状态 Joining, LeaderElection, Clustered
ClusterNode  集群节点,id必须全局唯一
NodeState 集群节点状态 Online, Offline

5. NIO相关
ProxyReactorThread  NIO Reactor Thread 负责多个Session会话 selector
NIOAcceptor NIO Acceptor Thread  selector



备注
1.集群端口
2.服务端口