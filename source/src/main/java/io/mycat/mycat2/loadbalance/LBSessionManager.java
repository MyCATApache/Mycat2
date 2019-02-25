package io.mycat.mycat2.loadbalance;

import java.io.IOException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.SessionManager;
import io.mycat.proxy.buffer.BufferPool;

/**
 * 负载均衡器前端会话管理器
 * <p>
 * Created by ynfeng on 2017/9/13.
 */
public class LBSessionManager implements SessionManager<LBSession> {
    private List<LBSession> allSession = new ArrayList<>();
    private static final NIOHandler<LBSession> defalultHandler = new LBNIOHandler();

    @Override
    public LBSession createSession(Object keyAttachement, BufferPool bufPool, Selector nioSelector,
                                      SocketChannel channel) throws IOException {
        LBSession lbSession = new LBSession(bufPool,nioSelector,channel);
        lbSession.setCurNIOHandler(getDefaultSessionHandler());
        allSession.add(lbSession);
        return lbSession;
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
