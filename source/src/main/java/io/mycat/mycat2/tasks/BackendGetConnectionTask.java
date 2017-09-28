package io.mycat.mycat2.tasks;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLSession;

public class BackendGetConnectionTask implements AsynTaskCallBack<MySQLSession>{
	
	private final CopyOnWriteArrayList<MySQLSession> successCons;
	private static final Logger logger = LoggerFactory.getLogger(BackendGetConnectionTask.class);
	private final AtomicInteger finishedCount = new AtomicInteger(0);
	private final int total;
	
	public BackendGetConnectionTask(CopyOnWriteArrayList<MySQLSession> connsToStore,
			int totalNumber) {
		this.successCons = connsToStore;
		this.total = totalNumber;
	}

	@Override
	public void finished(MySQLSession session, Object sender, boolean success, Object result) throws IOException {
		finishedCount.incrementAndGet();
		if(success){
			logger.info("connected successfuly " + session);
			successCons.add(session);
		}else{
			logger.warn("connected error " + session);
		}
	}
	
	public String getStatusInfo()
	{
		return "finished "+ finishedCount.get()+" success "+successCons.size()+" target count:"+this.total;
	}
	
	public boolean finished() {
		return finishedCount.get() >= total;
	}


}
