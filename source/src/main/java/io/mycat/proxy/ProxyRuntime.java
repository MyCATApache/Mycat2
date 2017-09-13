package io.mycat.proxy;
/**
 * 运行时环境，单例方式访问
 * @author wuzhihui
 *
 */

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.mycat.mycat2.loadbalance.LBSession;
import io.mycat.mycat2.loadbalance.LoadBalanceStrategy;
import io.mycat.mycat2.loadbalance.LoadChecker;
import io.mycat.mycat2.loadbalance.ProxySession;
import io.mycat.proxy.man.AdminCommandResovler;
import io.mycat.proxy.man.AdminSession;
import io.mycat.proxy.man.MyCluster;

public class ProxyRuntime {
	private ProxyConfig proxyConfig;
	public static final ProxyRuntime INSTANCE = new ProxyRuntime();
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
	//本地负载状态检查
	private LoadChecker localLoadChecker;
	private LoadBalanceStrategy loadBalanceStrategy;

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

	}

	public MyCluster getMyCLuster() {
		return myCLuster;
	}

	public void setMyCLuster(MyCluster myCLuster) {
		this.myCLuster = myCLuster;
	}

	public ProxyConfig getProxyConfig() {
		return proxyConfig;
	}

	public void setProxyConfig(ProxyConfig proxyConfig) {
		this.proxyConfig = proxyConfig;
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
	 * @param delayedSeconds
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

	public LoadChecker getLocalLoadChecker() {
		return localLoadChecker;
	}

	public void setLocalLoadChecker(LoadChecker localLoadChecker) {
		this.localLoadChecker = localLoadChecker;
	}

	public LoadBalanceStrategy getLoadBalanceStrategy() {
		return loadBalanceStrategy;
	}

	public void setLoadBalanceStrategy(LoadBalanceStrategy loadBalanceStrategy) {
		this.loadBalanceStrategy = loadBalanceStrategy;
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
