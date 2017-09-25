package io.mycat.proxy;

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

import io.mycat.mycat2.beans.conf.HeartbeatConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import io.mycat.mycat2.MycatConfig;
import io.mycat.mycat2.beans.MySQLRepBean;
import io.mycat.mycat2.common.ExecutorUtil;
import io.mycat.mycat2.common.NameableExecutor;
import io.mycat.mycat2.loadbalance.LBSession;
import io.mycat.mycat2.loadbalance.LoadBalanceStrategy;
import io.mycat.mycat2.loadbalance.ProxySession;
import io.mycat.proxy.man.AdminCommandResovler;
import io.mycat.proxy.man.AdminSession;
import io.mycat.proxy.man.MyCluster;
import io.mycat.util.TimeUtil;

public class ProxyRuntime {
	public static final ProxyRuntime INSTANCE = new ProxyRuntime();
	private static final Logger logger = LoggerFactory.getLogger(ProxyRuntime.class);

	/*
	 * 时间更新周期
	 */
	private static final long TIME_UPDATE_PERIOD   = 20L;
	private static final String TIME_UPDATE_TASK   = "TIME_UPDATE_TASK";
	private static final String PROCESSOR_CHECK    = "PROCESSOR_CHECK";
	private static final String REPLICA_ILDE_CHECK = "REPLICA_ILDE_CHECK";
	private static final String REPLICA_HEARTBEAT  = "REPLICA_HEARTBEAT";

	private MycatConfig config;
	private AtomicInteger sessionId = new AtomicInteger(1);
	private int nioReactorThreads = 2;
	private boolean traceProtocol = false;
	private final long startTime = System.currentTimeMillis();

	private NIOAcceptor acceptor;
	private ProxyReactorThread<?>[] reactorThreads;
	private SessionManager<?> sessionManager;
	// 用于管理端口的Session会话管理
	private SessionManager<AdminSession> adminSessionManager;
	private SessionManager<ProxySession> proxySessionSessionManager;
	private SessionManager<LBSession> lbSessionSessionManager;

	private AdminCommandResovler adminCmdResolver;
	private static final ScheduledExecutorService schedulerService;

	private NameableExecutor businessExecutor;
	private ListeningExecutorService listeningExecutorService;

	private Map<String,ScheduledFuture<?>> heartBeatTasks = new HashMap<>();
	private NameableExecutor timerExecutor;
	private ScheduledExecutorService heartbeatScheduler;
	
	public  long maxdataSourceInitTime = 60 * 1000L;
	
	
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
		//心跳调度独立出来，避免被其他任务影响
		heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();
		HeartbeatConfig heartbeatConfig = config.getConfig(ConfigEnum.HEARTBEAT);
		timerExecutor = ExecutorUtil.create("Timer", heartbeatConfig.getHeartbeat().getTimerExecutor());
		businessExecutor = ExecutorUtil.create("BusinessExecutor",Runtime.getRuntime().availableProcessors());
		listeningExecutorService = MoreExecutors.listeningDecorator(businessExecutor);
	}
	
	public ProxyReactorThread<?> getProxyReactorThread(ReactorEnv reactorEnv){
		// 找到一个可用的NIO Reactor Thread，交付托管
		if (reactorEnv.counter++ == Integer.MAX_VALUE) {
			reactorEnv.counter = 1;
		}
		int index = reactorEnv.counter % ProxyRuntime.INSTANCE.getNioReactorThreads();
		// 获取一个reactor对象
		return ProxyRuntime.INSTANCE.getReactorThreads()[index];
	}
	
	/**
	 * 启动心跳检测任务
	 */
	public void startHeartBeatScheduler(){
		if(heartBeatTasks.get(REPLICA_HEARTBEAT)==null){
			HeartbeatConfig heartbeatConfig = config.getConfig(ConfigEnum.HEARTBEAT);
			long replicaHeartbeat = heartbeatConfig.getHeartbeat().getReplicaHeartbeatPeriod();
			heartBeatTasks.put(REPLICA_HEARTBEAT,
					heartbeatScheduler.scheduleAtFixedRate(replicaHeartbeat(),
														  0,
														  replicaHeartbeat,
														  TimeUnit.MILLISECONDS));
		}
	}

	public void addBusinessJob(Runnable job) {
		businessExecutor.execute(job);
	}

	public void addDelayedJob(Runnable job, int delayedSeconds) {
		schedulerService.schedule(job, delayedSeconds, TimeUnit.SECONDS);
	}

	/**
	 * 切换 metaBean 名称
	 */
	public void startSwitchDataSource(String replBean,Integer writeIndex){
		MySQLRepBean repBean = config.getMySQLRepBean(replBean);
		
		if (repBean != null){
			addBusinessJob(() -> {
				repBean.setSwitchResult(false);
				repBean.switchSource(writeIndex,maxdataSourceInitTime);

				if (repBean.getSwitchResult().get()){
					logger.info("success to switch datasource for replica: {}, writeIndex: {}", repBean, writeIndex);
				} else {
					logger.error("error to switch datasource for replica: {}, writeIndex: {}", repBean, writeIndex);
				}
			});
		}	
	}
	
	/**
	 * 停止
	 */
	public void stopHeartBeatScheduler(){
		heartBeatTasks.values().stream().forEach(f->f.cancel(false));
		heartBeatTasks.clear();
	}
	
	// 系统时间定时更新任务
	private Runnable updateTime() {
		return new Runnable() {
			@Override
			public void run() {
				TimeUtil.update();
			}
		};
	}
		
	// 数据节点定时心跳任务
	private Runnable replicaHeartbeat() {
		return ()->{
			ProxyReactorThread<?> reactor  = getReactorThreads()[ThreadLocalRandom.current().nextInt(getReactorThreads().length)];
			reactor.addNIOJob(()-> config.getMysqlRepMap().values().stream().forEach(f -> f.doHeartbeat()));
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

	public int getNioReactorThreads() {
		return nioReactorThreads;
	}

	public void setNioReactorThreads(int nioReactorThreads) {
		this.nioReactorThreads = nioReactorThreads;
	}

	public ProxyReactorThread<?>[] getReactorThreads() {
		return reactorThreads;
	}

	public static boolean isNioBiproxyflag() {
		return nio_biproxyflag;
	}

	public void setReactorThreads(ProxyReactorThread<?>[] reactorThreads) {
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
}
