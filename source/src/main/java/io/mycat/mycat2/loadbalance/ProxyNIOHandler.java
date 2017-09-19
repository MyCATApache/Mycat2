package io.mycat.mycat2.loadbalance;

import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.ProxyBuffer;

import java.io.IOException;
import java.nio.channels.SelectionKey;

/**
 * Created by ynfeng on 2017/9/13.
 */
public class ProxyNIOHandler implements NIOHandler<ProxySession> {
    @Override
    public void onConnect(SelectionKey curKey, ProxySession session, boolean success, String msg) throws IOException {
        session.useSharedBuffer(session.getLbSession().proxyBuffer);
    }

    @Override
    public void onSocketRead(ProxySession session) throws IOException {
        if (session.readFromChannel()) {
            ProxyBuffer curBuffer = session.proxyBuffer;
            curBuffer.flip();
            curBuffer.readIndex = curBuffer.writeIndex;
            session.giveupOwner(SelectionKey.OP_WRITE);
            session.getLbSession().writeToChannel();
        }
    }

    @Override
    public void onSocketWrite(ProxySession session) throws IOException {
        session.writeToChannel();
    }

    @Override
    public void onWriteFinished(ProxySession session) throws IOException {
        session.proxyBuffer.flip();
        session.takeOwner(SelectionKey.OP_READ);
    }

    @Override
    public void onSocketClosed(ProxySession session, boolean normal) {

    }
}
