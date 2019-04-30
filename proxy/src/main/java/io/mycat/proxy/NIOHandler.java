package io.mycat.proxy;

import io.mycat.proxy.session.AbstractMySQLSession;
import io.mycat.proxy.session.MySQLSession;
import io.mycat.proxy.session.Session;

import java.io.IOException;
import java.nio.channels.SelectionKey;

public interface NIOHandler<T extends Session> {

    default void onConnect(SelectionKey curKey, T session, boolean success, Throwable throwable) throws IOException {
        throw new RuntimeException("not implemented ");
    }

    void onSocketRead(T session) throws IOException;

    default void onSocketWrite(T session) throws IOException {
        session.writeToChannel();
    }

    public void onWriteFinished(T session) throws IOException;

    void onSocketClosed(T session, boolean normal);


    default public T getCurrentMySQLSession() {
        MycatReactorThread thread = (MycatReactorThread) Thread.currentThread();
        return (T) thread.getReactorEnv().getCurSession();
    }

}
