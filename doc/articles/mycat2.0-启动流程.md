1. 程序的入口是io.mycat.mycat2.MycatCore.
在main 方法中 首选取得ProxyRuntime的实例,该类是一个单例模式.
2. 

```
runtime.setConfig(new MycatConfig());
```
该方法为runtime设置了一个MycatConfig ,MycatConfig 是负责读取配置文件的.

3. 
```
ConfigLoader.INSTANCE.loadCore();
```

调用ConfigLoader 进行配置文件的加载.

 3.1. 加载mycat.yml,heartbeat.yml,cluster.yml,balancer.yml,user.yml 的配置文件.代码如下:
 
```
public void loadCore() throws IOException {
    loadConfig(ConfigEnum.PROXY, GlobalBean.INIT_VERSION);
    loadConfig(ConfigEnum.HEARTBEAT, GlobalBean.INIT_VERSION);
    loadConfig(ConfigEnum.CLUSTER, GlobalBean.INIT_VERSION);
    loadConfig(ConfigEnum.BALANCER, GlobalBean.INIT_VERSION);
    loadConfig(ConfigEnum.USER, GlobalBean.INIT_VERSION);
}
```
 3.2 进行配置文件的加载,调用了YamlUtil进行加载,并将配置信息赋值给MycatConfig,关键代码如下 :
 
```
  conf.putConfig(configEnum, (Configurable) YamlUtil.load(fileName, configEnum.getClazz()), version);
```
 3.3 在io.mycat.util.YamlUtil.load(String, Class<T>) 中,处理也很简单,通过Yaml进行文件的加载,然后通过反射机制将属性进行赋值.代码如下:
 
```
/**
 * 从指定的文件中加载配置
 * @param fileName 需要加载的文件名
 * @param clazz 加载后需要转换成的类对象
 * @return
 * @throws FileNotFoundException
 */
public static <T> T load(String fileName, Class<T> clazz) throws FileNotFoundException {
    InputStreamReader fis = null;
    try {
        URL url = YamlUtil.class.getClassLoader().getResource(fileName);
        if (url != null) {
            Yaml yaml = new Yaml();
            fis = new InputStreamReader(new FileInputStream(url.getFile()), StandardCharsets.UTF_8);
            T obj = yaml.loadAs(fis, clazz);
            return obj;
        }
        return null;
    } finally {
        if (fis != null) {
            try {
                fis.close();
            } catch (IOException ignored) {
            }
        }
    }
}
```
4. 进行通过命令行传入参数的解析.如:可以传入
    -  -mycat.proxy.port 8067
    -  -mycat.cluster.enable true
    -  -mycat.cluster.port 9067
    -  -mycat.cluster.myNodeId leader-2
    
参数等 
支持的参数可在 io.mycat.mycat2.beans.ArgsBean 中进行查看.
 
 获得参数后通过遍历进行赋值操作即可,代码如下:

```
private static void solveArgs(String[] args) {
int lenght = args.length;

MycatConfig conf = ProxyRuntime.INSTANCE.getConfig();
ProxyConfig proxyConfig = conf.getConfig(ConfigEnum.PROXY);
ClusterConfig clusterConfig = conf.getConfig(ConfigEnum.CLUSTER);
BalancerConfig balancerConfig= conf.getConfig(ConfigEnum.BALANCER);

for (int i = 0; i < lenght; i++) {
	switch(args[i]) {
		case ArgsBean.PROXY_PORT:
			proxyConfig.getProxy().setPort(Integer.parseInt(args[++i]));
			break;
		case ArgsBean.CLUSTER_ENABLE:
			clusterConfig.getCluster().setEnable(Boolean.parseBoolean(args[++i]));
			break;
		case ArgsBean.CLUSTER_PORT:
			clusterConfig.getCluster().setPort(Integer.parseInt(args[++i]));
			break;
		case ArgsBean.CLUSTER_MY_NODE_ID:
			clusterConfig.getCluster().setMyNodeId(args[++i]);
			break;
		case ArgsBean.BALANCER_ENABLE:
			balancerConfig.getBalancer().setEnable(Boolean.parseBoolean(args[++i]));
			break;
		case ArgsBean.BALANCER_PORT:
			balancerConfig.getBalancer().setPort(Integer.parseInt(args[++i]));
			break;
		case ArgsBean.BALANCER_STRATEGY:
			BalancerBean.BalancerStrategyEnum strategy = BalancerBean.BalancerStrategyEnum.getEnum(args[++i]);
			if (strategy == null) {
				throw new IllegalArgumentException("no such balancer strategy");
			}
			balancerConfig.getBalancer().setStrategy(strategy);
			break;
		default:
			break;
	}
}
}
```

 

5. 设置NioReactorThreads,线程数目按照cpu的数目而定.

```
int cpus = Runtime.getRuntime().availableProcessors();
runtime.setNioReactorThreads(cpus);
runtime.setReactorThreads(new MycatReactorThread[cpus]);
```
6.设置MycatSessionManager

7. 然后调用io.mycat.proxy.ProxyRuntime 的init方法,进行资源的初始化.代码如下:

```
public void init() {
//心跳调度独立出来，避免被其他任务影响
heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();
HeartbeatConfig heartbeatConfig = config.getConfig(ConfigEnum.HEARTBEAT);
timerExecutor = ExecutorUtil.create("Timer", heartbeatConfig.getHeartbeat().getTimerExecutor());
businessExecutor = ExecutorUtil.create("BusinessExecutor",Runtime.getRuntime().availableProcessors());
listeningExecutorService = MoreExecutors.listeningDecorator(businessExecutor);
MatchMethodGenerator.initShrinkCharTbl();
}
```
7.1. 建立心跳线程, 原因是避免被其他任务影响.

7.2  建立timerExecutor,线程数目默认为2.

7.3  建立businessExecutor,线程数目为cpu数目.

7.4  建立listeningExecutorService, 该ExecutorService将会进行任务的提交给businessExecutor

7.5 调用io.mycat.mycat2.sqlparser.MatchMethodGenerator#initShrinkCharTbl方法.该方法只是对0-9a-zA-Z的字符进行映射.代码如下:


```
static final byte[] shrinkCharTbl = new byte[96];//为了压缩hash字符映射空间，再次进行转义
public static void initShrinkCharTbl () {
    shrinkCharTbl[0] = 1;//从 $ 开始计算
    IntStream.rangeClosed('0', '9').forEach(c -> shrinkCharTbl[c-'$'] = (byte)(c-'0'+2));
    IntStream.rangeClosed('A', 'Z').forEach(c -> shrinkCharTbl[c-'$'] = (byte)(c-'A'+12));
    IntStream.rangeClosed('a', 'z').forEach(c -> shrinkCharTbl[c-'$'] = (byte)(c-'a'+12));
    shrinkCharTbl['_'-'$'] = (byte)38;
}
```
8. 调用ProxyStarter的start,首先 是创建了NIOAcceptor,负责对请求进行处理.然后根据ClusterConfig的配置进行相应的处理.


```
ClusterConfig clusterConfig = conf.getConfig(ConfigEnum.CLUSTER);
ClusterBean clusterBean = clusterConfig.getCluster();
if (clusterBean.isEnable()) {
	// 启动集群
	startCluster(runtime, clusterBean, acceptor);
} else {
	// 未配置集群，直接启动
	startProxy(true);
}
```

9.接下来分析未配置集群的启动方式.

9.1 首先通过ProxyRuntime获取到ProxyConfig(该对象是mycat.yml的封装)，mycat.yml的配置文件如下:

```
proxy:
  ip: 0.0.0.0
  port: 8066
```
因此可以通过该类拿到启动端口和ip.因此传入NIOAcceptor进行监听.

9.2 在io.mycat.proxy.NIOAcceptor#startServerChannel中,做了如下处理:

9.2.1 首先检查ServerSocketChannel是否已经启动,如果已经启动,不进行后续处理.

9.2.2 根据传入的ServerType做不同的处理,当前我们传入的值为MYCAT. 因此不进行处理.

```
if (serverType == ServerType.CLUSTER) {
	adminSessionMan = new DefaultAdminSessionManager();
	ProxyRuntime.INSTANCE.setAdminSessionManager(adminSessionMan);
	logger.info("opend cluster conmunite port on {}:{}", ip, port);
} else if (serverType == ServerType.LOAD_BALANCER){
	logger.info("opend load balance conmunite port on {}:{}", ip, port);
	ProxyRuntime.INSTANCE.setProxySessionSessionManager(new ProxySessionManager());
	ProxyRuntime.INSTANCE.setLbSessionSessionManager(new LBSessionManager());
}
```

9.2.3 调用io.mycat.proxy.NIOAcceptor#openServerChannel,进行启动.代码如下:


```
private void openServerChannel(Selector selector, String bindIp, int bindPort, ServerType serverType)
	throws IOException {
final ServerSocketChannel serverChannel = ServerSocketChannel.open();
final InetSocketAddress isa = new InetSocketAddress(bindIp, bindPort);
serverChannel.bind(isa);
serverChannel.configureBlocking(false);
serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
serverChannel.register(selector, SelectionKey.OP_ACCEPT, serverType);
if (serverType == ServerType.CLUSTER) {
	logger.info("open cluster server port on {}:{}", bindIp, bindPort);
	clusterServerSocketChannel = serverChannel;
} else if (serverType == ServerType.LOAD_BALANCER) {
	logger.info("open load balance server port on {}:{}", bindIp, bindPort);
	loadBalanceServerSocketChannel = serverChannel;
} else {
	logger.info("open proxy server port on {}:{}", bindIp, bindPort);
	proxyServerSocketChannel = serverChannel;
}
}
```

9.4 调用io.mycat.mycat2.ProxyStarter#startReactor,进行MycatReactorThread的启动.该线程负责对请求进行处理.

9.5 加载配置文件信息,该方法在ROOT_PATH目录下创建了prepare和archive两个文件夹,并加载了replica-index.yml,datasource.yml,schema.yml,最后调用了AnnotationProcessor#getInstance  进行初始化.

9.5.1 创建DynamicAnnotationManager.

9.5.2 对AnnotationProcessor.class.getClassLoader().getResource("") 的路径进行监听.当文件目录有变化时会回调io.mycat.mycat2.sqlannotations.AnnotationProcessor#listen方法.代码如下:


```
public static void listen() {
try {
while (true) {
    WatchKey key = watcher.take();//todo 线程复用,用 poll比较好?
    boolean flag = false;
    for (WatchEvent<?> event: key.pollEvents()) {
        String str = event.context().toString();
        if ("actions.yml".equals(str)|| "annotations.yml".equals(str)) {
            flag=true;
            break;
        }
    }
    if (flag){
        System.out.println("动态注解更新次数" + count.incrementAndGet());
        init();
    }
    boolean valid = key.reset();
    if (!valid) {
        break;
    }

}
} catch (Exception e) {
e.printStackTrace();
}
}
```

==可以看到,当actions.yml或者annotations.yml变化时,就会重新调用init方法,从而对DynamicAnnotationManager进行修改.==


9.6 分别对datasource.yml 和 schema.yml 进行加载.

9.6.1 对MySQLRepBean进行初始化.该bean是对datasource.yml的封装.datasource.yml如下所示:


```
replicas:
  - name: test                      # 复制组 名称   必须唯一
    repType: MASTER_SLAVE           # 复制类型
    switchType: SWITCH              # 切换类型
    balanceType: BALANCE_ALL_READ   # 读写分离类型
    mysqls:            
      - hostName: mysql-01              # mysql 主机名
        ip: 127.0.0.1               # ip
        port: 3306                  # port
        user: root                  # 用户名
        password: root            # 密码
        minCon: 1                   # 最小连接
        maxCon: 10                  # 最大连接
        maxRetryCount: 3            # 连接重试次数

```


```
conf.getMysqlRepMap().forEach((repName, repBean) -> {
	repBean.initMaster();
	repBean.getMetaBeans().forEach(metaBean -> metaBean.prepareHeartBeat(repBean, repBean.getDataSourceInitStatus()));
});
```

9.6.2 调用io.mycat.mycat2.beans.MySQLRepBean#initMaster, 做了如下处理:

首先加载replica-index.yml,获取replica name 对应的index，并对MySQLRepBean中的metaBeans进行赋值,完成对mysqls 的封装.

之后调用MySQLMetaBean的prepareHeartBeat方法.
完成


9.7 ==调用io.mycat.proxy.ProxyRuntime#startHeartBeatScheduler,启动heartbeatScheduler线程.该线程会每10000 ms 调用io.mycat.proxy.ProxyRuntime#replicaHeartbeat.== 心跳的配置在heartbeat.yml中,默认配置如下:


```
heartbeat:
  timerExecutor: 2
  replicaHeartbeatPeriod: 10000
  replicaIdleCheckPeriod: 2000
  idleTimeout: 2000
  processorCheckPeriod: 2000
  minSwitchtimeInterval: 120000
```

9.7.1 在io.mycat.proxy.ProxyRuntime#replicaHeartbeat,方法中代码如下:


```
private Runnable replicaHeartbeat() {
return ()->{
ProxyReactorThread<?> reactor  = getReactorThreads()[ThreadLocalRandom.current().nextInt(getReactorThreads().length)];
reactor.addNIOJob(()-> config.getMysqlRepMap().values().stream().forEach(f -> f.doHeartbeat()));
};
}
```

最终会调用io.mycat.mycat2.beans.heartbeat.MySQLHeartbeat#heartbeat.


9.8 对cluster的配置进行处理.代码如下:


```
BalancerConfig balancerConfig = conf.getConfig(ConfigEnum.BALANCER);
BalancerBean balancerBean = balancerConfig.getBalancer();
// 集群模式下才开启负载均衡服务
if (clusterBean.isEnable() && balancerBean.isEnable()) {
	runtime.getAcceptor().startServerChannel(balancerBean.getIp(), balancerBean.getPort(), ServerType.LOAD_BALANCER);
}
```

