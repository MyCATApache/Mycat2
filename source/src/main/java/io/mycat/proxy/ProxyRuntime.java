package io.mycat.proxy;

import java.io.File;
/**
 * 运行时环境，单例方式访问
 * @author wuzhihui
 *
 */
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.mycat.mycat2.beans.MySQLMetaBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import io.mycat.mycat2.MycatConfig;
import io.mycat.mycat2.beans.MySQLRepBean;
import io.mycat.mycat2.beans.conf.ClusterConfig;
import io.mycat.mycat2.beans.conf.HeartbeatConfig;
import io.mycat.mycat2.beans.conf.ReplicaIndexConfig;
import io.mycat.mycat2.common.ExecutorUtil;
import io.mycat.mycat2.common.NameableExecutor;
import io.mycat.mycat2.loadbalance.LBSession;
import io.mycat.mycat2.loadbalance.ProxySession;
import io.mycat.mycat2.sqlparser.MatchMethodGenerator;
import io.mycat.proxy.buffer.BufferPooLFactory;
import io.mycat.proxy.man.AdminCommandResovler;
import io.mycat.proxy.man.AdminSession;
import io.mycat.proxy.man.MyCluster;
import io.mycat.proxy.man.cmds.ConfigUpdatePacketCommand;
import io.mycat.util.TimeUtil;
import io.mycat.util.YamlUtil;
import io.mycat.util.classloader.DynaClassLoader;

public class ProxyRuntime {
	public static final ProxyRuntime INSTANCE = new ProxyRuntime();
	private static final Logger logger = LoggerFactory.getLogger(ProxyRuntime.class);

	/*
	 * 时间更新周期
	 */
	private static final long TIME_UPDATE_PERIOD = 20L;
	private static final String TIME_UPDATE_TASK = "TIME_UPDATE_TASK";
	private static final String PROCESSOR_CHECK = "PROCESSOR_CHECK";
	private static final String REPLICA_ILDE_CHECK = "REPLICA_ILDE_CHECK";
	private static final String REPLICA_HEARTBEAT = "REPLICA_HEARTBEAT";

	private MycatConfig config;
	private AtomicInteger sessionId = new AtomicInteger(1);
	private boolean traceProtocol = false;
	private final long startTime = System.currentTimeMillis();

	private NIOAcceptor acceptor;
	// Mycat 8066数据端口线程派发池
	private MycatReactorThread[] reactorThreads;
	// 负载均衡器所用的Reactor派发线程池
	private ProxyReactorThread<LBSession>[] lbReactorThreads;
	private SessionManager<?> sessionManager;
	// 用于管理端口的Session会话管理
	private SessionManager<AdminSession> adminSessionManager;
	private SessionManager<ProxySession> proxySessionSessionManager;
	private SessionManager<LBSession> lbSessionSessionManager;

	private AdminCommandResovler adminCmdResolver;
	private static final ScheduledExecutorService schedulerService;

	private NameableExecutor businessExecutor;
	private ListeningExecutorService listeningExecutorService;

	private Map<String, ScheduledFuture<?>> heartBeatTasks = new HashMap<>();
	private NameableExecutor timerExecutor;
	private ScheduledExecutorService heartbeatScheduler;

	public long maxdataSourceInitTime = 60 * 1000L;
	private int catletClassCheckSeconds = 60;
	/* 动态加载catlet的classs */
	private DynaClassLoader catletLoader = null;
	private BufferPooLFactory bufferPoolFactory = null;

	/**
	 * 是否双向同时通信，大部分TCP Server是单向的，即发送命令，等待应答，然后下一个
	 */
	private static final boolean nio_biproxyflag = false;
	static {
		// todo ,from properties to load class name
		// nioProxyHandler=new MySQLProxyHandler();
		// nioProxyHandler=new DefaultDirectProxyHandler();
		// todo from proerpteis to load pool size param
		schedulerService = Executors.newScheduledThreadPool(1);
	}

	private MyCluster myCLuster;

	public void init() {
		// 心跳调度独立出来，避免被其他任务影响
		heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();
		HeartbeatConfig heartbeatConfig = config.getConfig(ConfigEnum.HEARTBEAT);
		timerExecutor = ExecutorUtil.create("Timer", heartbeatConfig.getHeartbeat().getTimerExecutor());
		businessExecutor = ExecutorUtil.create("BusinessExecutor", Runtime.getRuntime().availableProcessors());
		listeningExecutorService = MoreExecutors.listeningDecorator(businessExecutor);
		MatchMethodGenerator.initShrinkCharTbl();

		// catletLoader = new
		// DynaClassLoader("C:\\Users\\netinnet\\Documents\\GitHub\\tcp-proxy\\source\\target\\classes\\catlet",
		// catletClassCheckSeconds);
//		catletLoader = new DynaClassLoader(YamlUtil.getRootHomePath() + File.separator + "catlet",
//				catletClassCheckSeconds);

		heartbeatScheduler.scheduleAtFixedRate(updateTime(), 0L, TIME_UPDATE_PERIOD, TimeUnit.MILLISECONDS);

		bufferPoolFactory = BufferPooLFactory.getInstance();
	}

	/**
	 * 启动心跳检测任务
	 */
	public void startHeartBeatScheduler() {
		if (heartBeatTasks.get(REPLICA_HEARTBEAT) == null) {
			HeartbeatConfig heartbeatConfig = config.getConfig(ConfigEnum.HEARTBEAT);
			long replicaHeartbeat = heartbeatConfig.getHeartbeat().getReplicaHeartbeatPeriod();
			heartBeatTasks.put(REPLICA_HEARTBEAT, heartbeatScheduler.scheduleAtFixedRate(replicaHeartbeat(), 0,
					replicaHeartbeat, TimeUnit.MILLISECONDS));
		}
	}

	public void addBusinessJob(Runnable job) {
		businessExecutor.execute(job);
	}

	public void addDelayedJob(Runnable job, int delayedSeconds) {
		schedulerService.schedule(job, delayedSeconds, TimeUnit.SECONDS);
	}

	/**
	 * 准备切换
	 *
	 * @param replBean
	 * @param writeIndex
	 */
	public void prepareSwitchDataSource(String replBean, Integer writeIndex, boolean sync) {
		MycatConfig conf = ProxyRuntime.INSTANCE.getConfig();
		ClusterConfig clusterConfig = conf.getConfig(ConfigEnum.CLUSTER);
		ReplicaIndexConfig curRepIndexConfig = conf.getConfig(ConfigEnum.REPLICA_INDEX);
		if (clusterConfig.getCluster().isEnable()) {
			ReplicaIndexConfig newRepIndexConfig = new ReplicaIndexConfig();
			Map<String, Integer> map = new HashMap(curRepIndexConfig.getReplicaIndexes());
			map.put(replBean, writeIndex);
			newRepIndexConfig.setReplicaIndexes(map);
			ConfigUpdatePacketCommand.INSTANCE.sendPreparePacket(ConfigEnum.REPLICA_INDEX, newRepIndexConfig, replBean);
		} else {
			// 非集群下直接更新replica-index信息
			ConfigEnum configEnum = ConfigEnum.REPLICA_INDEX;
			curRepIndexConfig.getReplicaIndexes().put(replBean, writeIndex);
			int curVersion = conf.getConfigVersion(configEnum);
			conf.setConfigVersion(configEnum, curVersion + 1);
			YamlUtil.archiveAndDumpToFile(conf.getConfig(configEnum), configEnum.getFileName(), curVersion);
			startSwitchDataSource(replBean, writeIndex, sync);
		}
	}

	/**
	 * 切换 metaBean 名称
	 */
	public void startSwitchDataSource(String replBean, Integer writeIndex, boolean sync) {

		MySQLRepBean repBean = config.getMySQLRepBean(replBean);

		Runnable runnable = () -> {
			repBean.setSwitchResult(false);
			repBean.switchSource(writeIndex, maxdataSourceInitTime);

			if (repBean.getSwitchResult().get()) {
				logger.info("success to switch datasource for replica: {}, writeIndex: {}",
						repBean.getReplicaBean().getName(), writeIndex);
			} else {
				logger.error("error to switch datasource for replica: {}, writeIndex: {}",
						repBean.getReplicaBean().getName(), writeIndex);
			}
		};

		if (repBean != null) {
			if (sync) {
				addBusinessJob(runnable);
			} else {
				runnable.run();
			}
		}
	}

	/**
	 * 停止
	 */
	public void stopHeartBeatScheduler() {
		heartBeatTasks.values().stream().forEach(f -> f.cancel(false));
		heartBeatTasks.clear();
	}

	// 系统时间定时更新任务
	public Runnable updateTime() {
		return new Runnable() {
			@Override
			public void run() {
				TimeUtil.update();
			}
		};
	}

	// 数据节点定时心跳任务
	private Runnable replicaHeartbeat() {
		return () -> {
			ProxyReactorThread<?> reactor = this.reactorThreads[ThreadLocalRandom.current()
					.nextInt(reactorThreads.length)];
			HeartbeatConfig config = this.config.getConfig(ConfigEnum.HEARTBEAT);
			long minHeartbeatChecktime = config.getHeartbeat().getMinHeartbeatChecktime();
			long now = System.currentTimeMillis();
			long l = now - minHeartbeatChecktime;
			for (MySQLRepBean value : this.config.getMysqlRepMap().values()) {
				for (MySQLMetaBean metaBean : value.getMetaBeans()) {
					boolean active = !(metaBean.getHeartbeat().getLastActiveTime() + minHeartbeatChecktime < now);
					if(!metaBean.getHeartbeat().isChecking()&&!active){
						reactor.addNIOJob(() ->metaBean.doHeartbeat());
					}
				}
			}


		};
	}

	public MyCluster getMyCLuster() {
		return myCLuster;
	}

	public void setMyCLuster(MyCluster myCLuster) {
		this.myCLuster = myCLuster;
	}

	public MycatConfig getConfig() {
		return config;
	}

	public void setConfig(MycatConfig config) {
		this.config = config;
	}

	public static ScheduledExecutorService getSchedulerservice() {
		return schedulerService;
	}

	public MycatReactorThread[] getMycatReactorThreads() {
		return reactorThreads;
	}

	public static boolean isNioBiproxyflag() {
		return nio_biproxyflag;
	}

	public void setMycatReactorThreads(MycatReactorThread[] reactorThreads) {
		this.reactorThreads = reactorThreads;
	}

	public SessionManager<?> getSessionManager() {
		return sessionManager;
	}

	public void setSessionManager(SessionManager<?> sessionManager) {
		this.sessionManager = sessionManager;
	}

	/**
	 * 在NIO主线程中调用的任务，通常涉及到ByteBuffer的操作与状态的改变，必须通过这种方式完成数据的交换与同步逻辑！！！
	 * 
	 * @param job
	 */
	public void addNIOJob(Runnable job) {
		if (Thread.currentThread() instanceof ProxyReactorThread) {
			((ProxyReactorThread<?>) Thread.currentThread()).addNIOJob(job);
		} else {
			throw new RuntimeException("Must  called in ProxyReactorThread ");
		}
	}

	public void addNIOJob(Runnable job, ProxyReactorThread<?> nioThread) {
		nioThread.addNIOJob(job);
	}

	public int genSessionId() {
		int val = sessionId.incrementAndGet();
		if (val < 0) {
			synchronized (sessionId) {
				if (sessionId.get() < 0) {
					int newValue = 1;
					sessionId.set(newValue);
					return newValue;
				} else {
					return sessionId.incrementAndGet();
				}
			}
		}
		return val;
	}

	/**
	 * 在NIO主线程中调度的延迟任务，即从当前时间开始，延迟N秒后才执行
	 * 
	 * @param job
	 * @param delayedSeconds
	 */
	public void addDelayedNIOJob(Runnable job, int delayedSeconds, ProxyReactorThread<?> nioThread) {
		schedulerService.schedule(() -> {
			nioThread.addNIOJob(job);
		}, delayedSeconds, TimeUnit.SECONDS);
	}

	/**
	 * 在NIO主线程中调度的延迟任务，重复执行
	 * 
	 * @param job
	 * @param initialDelay
	 * @param period
	 * @param nioThread
	 */
	public void addCronNIOJob(Runnable job, int initialDelay, int period, ProxyReactorThread<?> nioThread) {
		schedulerService.scheduleWithFixedDelay(() -> {
			nioThread.addNIOJob(job);
		}, initialDelay, period, TimeUnit.SECONDS);
	}

	public boolean isTraceProtocol() {
		return traceProtocol;
	}

	public void setTraceProtocol(boolean traceProtocol) {
		this.traceProtocol = traceProtocol;
	}

	public SessionManager<AdminSession> getAdminSessionManager() {
		return adminSessionManager;
	}

	public void setAdminSessionManager(SessionManager<AdminSession> adminSessionManager) {
		this.adminSessionManager = adminSessionManager;
	}

	public ProxyReactorThread<LBSession>[] getLbReactorThreads() {
		return lbReactorThreads;
	}

	public void setLbReactorThreads(ProxyReactorThread<LBSession>[] lbReactorThreads) {
		this.lbReactorThreads = lbReactorThreads;
	}

	public long getStartTime() {
		return startTime;
	}

	public AdminCommandResovler getAdminCmdResolver() {
		return adminCmdResolver;
	}

	public void setAdminCmdResolver(AdminCommandResovler adminCmdResolver) {
		this.adminCmdResolver = adminCmdResolver;
	}

	public NIOAcceptor getAcceptor() {
		return acceptor;
	}

	public void setAcceptor(NIOAcceptor acceptor) {
		this.acceptor = acceptor;
	}

	public SessionManager<ProxySession> getProxySessionSessionManager() {
		return proxySessionSessionManager;
	}

	public void setProxySessionSessionManager(SessionManager<ProxySession> proxySessionSessionManager) {
		this.proxySessionSessionManager = proxySessionSessionManager;
	}

	public SessionManager<LBSession> getLbSessionSessionManager() {
		return lbSessionSessionManager;
	}

	public void setLbSessionSessionManager(SessionManager<LBSession> lbSessionSessionManager) {
		this.lbSessionSessionManager = lbSessionSessionManager;
	}

	public DynaClassLoader getCatletLoader() {
		return catletLoader;
	}

	public BufferPooLFactory getBufferPoolFactory() {
		return bufferPoolFactory;
	}

	public void setBufferPoolFactory(BufferPooLFactory bufferPoolFactory) {
		this.bufferPoolFactory = bufferPoolFactory;
	}

	public NameableExecutor getTimerExecutor() {
		return this.timerExecutor;
	}

	public NameableExecutor getBusinessExecutor() {
		return this.businessExecutor;
	}
}
