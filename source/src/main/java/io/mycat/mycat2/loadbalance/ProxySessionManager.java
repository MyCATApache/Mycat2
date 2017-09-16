package io.mycat.mycat2.loadbalance;

import io.mycat.proxy.BufferPool;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.Session;
import io.mycat.proxy.SessionManager;

import java.io.IOException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * 负载均衡器后端会话管理器
 * <p>
 * Created by ynfeng on 2017/9/13.
 */
public class ProxySessionManager implements SessionManager<ProxySession> {
    private List<ProxySession> allSession = new ArrayList<>();
    private static final NIOHandler<ProxySession> defalultHandler = new ProxyNIOHandler();

    @Override
    public ProxySession createSession(Object keyAttachement, BufferPool bufPool, Selector nioSelector,
                                      SocketChannel channel, boolean isAcceptedCon) throws IOException {
        ProxySession proxySession = new ProxySession(bufPool, nioSelector, channel);
        proxySession.setCurNIOHandler(getDefaultSessionHandler());
        allSession.add(proxySession);
        return proxySession;
    }

    @Override
    public Collection<ProxySession> getAllSessions() {
        return Collections.unmodifiableCollection(allSession);
    }

    @Override
    public NIOHandler getDefaultSessionHandler() {
        return defalultHandler;
    }

    @Override
    public void removeSession(Session session) {
        allSession.remove(session);
    }
}
