package io.mycat.mycat2.loadbalance;

import java.io.IOException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import io.mycat.mycat2.tasks.AsynTaskCallBack;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.Session;
import io.mycat.proxy.SessionManager;
import io.mycat.proxy.buffer.BufferPool;

/**
 * 负载均衡器后端会话管理器
 * <p>
 * Created by ynfeng on 2017/9/13.
 */
public class ProxySessionManager implements SessionManager<ProxySession> {
    private List<ProxySession> allSession = new ArrayList<>();
    private static final NIOHandler<ProxySession> defalultHandler = new ProxyNIOHandler();


    @Override
    public void createSession(Object keyAttachement, BufferPool bufPool, Selector nioSelector, SocketChannel channel, AsynTaskCallBack<ProxySession> callBack) throws IOException {
        ProxySession proxySession = new ProxySession(bufPool, nioSelector, channel,getDefaultSessionHandler());
        allSession.add(proxySession);
        if (callBack!=null){
            callBack.finished(proxySession,this,channel.isConnected(),null);
        }
    }

    @Override
    public Collection<ProxySession> getAllSessions() {
        return Collections.unmodifiableCollection(allSession);
    }

    @Override
    public NIOHandler<ProxySession> getDefaultSessionHandler() {
        return defalultHandler;
    }

    @Override
    public void removeSession(ProxySession session) {
        allSession.remove(session);
    }

	@Override
	public int curSessionCount() {
		return allSession.size();
	}
}
