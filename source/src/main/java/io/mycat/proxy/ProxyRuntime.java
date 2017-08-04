package io.mycat.proxy;
/**
 * 杩愯鏃剁殑涓�浜涗俊鎭紝鍗曚緥妯″紡锛屼緵鍏ㄥ眬浣跨敤
 * @author wuzhihui
 *
 */

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ProxyRuntime {

	public static final ProxyRuntime INSTANCE = new ProxyRuntime();

	private String bindIP;
	private int bindPort;
	private int nioReactorThreads = 2;
	private ProxyReactorThread[] reactorThreads;

	private ScheduledExecutorService schedulerService;
	public void init()
	{
		schedulerService = Executors.newScheduledThreadPool(1);
		
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
	/**
	 * 在NIO主线程中调度的延迟任务，即从当前时间开始，延迟N秒后才执行
	 * @param job
	 * @param delayedSeconds 
	 */
	public void addDelayedNIOJob(Runnable job,int delayedSeconds,ProxyReactorThread nioThread)
	{
		schedulerService.schedule(()->{nioThread.addNIOJob(job);}, delayedSeconds, TimeUnit.SECONDS);
	}
	/**
	 * 在NIO主线程中调度的延迟任务，重复执行
	 * @param job
	 * @param delayedSeconds
	 * @param nioThread
	 */
	public void addCronNIOJob(Runnable job,int initialDelay,int period,ProxyReactorThread nioThread)
	{
		schedulerService.scheduleWithFixedDelay(()->{nioThread.addNIOJob(job);}, initialDelay,period, TimeUnit.SECONDS);
	}
}
