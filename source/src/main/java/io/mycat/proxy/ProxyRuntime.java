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

import io.mycat.mycat2.MySQLProxyHandler;
import io.mycat.mycat2.MySQLProxyStudyHandler;
import io.mycat.mycat2.cmd.DirectPassSQLProcessor;

public class ProxyRuntime {

	public static final ProxyRuntime INSTANCE = new ProxyRuntime();
	private AtomicInteger sessionId = new AtomicInteger(1);
	private String bindIP;
	private int bindPort;
	private int nioReactorThreads = 2;
	private boolean traceProtocol=true;
	private ProxyReactorThread[] reactorThreads;
    private static final NIOProxyHandler nioProxyHandler;
	private static final ScheduledExecutorService schedulerService;

	static
	{
		//todo ,from properties to load class name
		//nioProxyHandler=new MySQLProxyStudyHandler();
		nioProxyHandler=new MySQLProxyHandler();
		//nioProxyHandler=new DefaultDirectProxyHandler();
		//todo from proerpteis to load pool size param
		schedulerService = Executors.newScheduledThreadPool(1);
	}
	public void init() {
		

	}

	public String getBindIP() {
		return bindIP;
	}

	public void setBindIP(String bindIP) {
		this.bindIP = bindIP;
	}

	public int getBindPort() {
		return bindPort;
	}

	public void setBindPort(int bindPort) {
		this.bindPort = bindPort;
	}

	public int getNioReactorThreads() {
		return nioReactorThreads;
	}

	public void setNioReactorThreads(int nioReactorThreads) {
		this.nioReactorThreads = nioReactorThreads;
	}

	public ProxyReactorThread[] getReactorThreads() {
		return reactorThreads;
	}

	public void setReactorThreads(ProxyReactorThread[] reactorThreads) {
		this.reactorThreads = reactorThreads;
	}

	public NIOProxyHandler getNioProxyHandler() {
		return nioProxyHandler;
	}

	

	/**
	 * 在NIO主线程中调用的任务，通常涉及到ByteBuffer的操作与状态的改变，必须通过这种方式完成数据的交换与同步逻辑！！！
	 * 
	 * @param job
	 */
	public void addNIOJob(Runnable job) {
		if (Thread.currentThread() instanceof ProxyReactorThread) {
			((ProxyReactorThread) Thread.currentThread()).addNIOJob(job);
		} else {
			throw new RuntimeException("Must  called in ProxyReactorThread ");
		}
	}

	public void addNIOJob(Runnable job, ProxyReactorThread nioThread) {
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
	public void addDelayedNIOJob(Runnable job, int delayedSeconds, ProxyReactorThread nioThread) {
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
	public void addCronNIOJob(Runnable job, int initialDelay, int period, ProxyReactorThread nioThread) {
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
	
}
