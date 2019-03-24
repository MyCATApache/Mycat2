package io.mycat.mycat2.loadbalance;

import io.mycat.mycat2.tasks.AsynTaskCallBack;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.SessionManager;
import io.mycat.proxy.buffer.BufferPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * 负载均衡器前端会话管理器
 * <p>
 * Created by ynfeng on 2017/9/13.
 */
public class LBSessionManager implements SessionManager<LBSession> {
    protected static Logger logger = LoggerFactory.getLogger(LBSessionManager.class);
    private List<LBSession> allSession = new ArrayList<>();
    private static final NIOHandler<LBSession> defalultHandler = new LBNIOHandler();



    @Override
    public void createSession(Object keyAttachement, BufferPool bufPool, Selector nioSelector, SocketChannel channel, AsynTaskCallBack<LBSession> callBack) throws IOException {
        if (logger.isInfoEnabled()) {
            if (channel.isConnected()){
                throw new RuntimeException("LBSession is not connected "+channel);
            }
        }
        LBSession lbSession = new LBSession(bufPool,nioSelector,channel,getDefaultSessionHandler());
        allSession.add(lbSession);
        if (callBack!=null){
            callBack.finished(lbSession,this,channel.isConnected(),null);
        }
    }

    @Override
    public Collection<LBSession> getAllSessions() {
        return Collections.unmodifiableCollection(allSession);
    }

    @Override
    public NIOHandler<LBSession>  getDefaultSessionHandler() {
        return defalultHandler;
    }

    @Override
    public void removeSession(LBSession session) {
        allSession.remove(session);
    }

	@Override
	public int curSessionCount() {
		return allSession.size();
	}
}
