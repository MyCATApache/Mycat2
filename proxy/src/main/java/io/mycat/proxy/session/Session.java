package io.mycat.proxy.session;

import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.buffer.ProxyBuffer;
import io.mycat.proxy.task.AsynTaskCallBack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SocketChannel;

public interface Session<T extends Session> {
    final static Logger logger = LoggerFactory.getLogger(Session.class);
    /**
     * 获取前端连接，前端连接为客户端主动连接到本Server的连接
     *
     * @return
     */
    public SocketChannel channel();

    boolean isClosed();

    // 当前NIO ProxyHandler
    public NIOHandler getCurNIOHandler();

    /**
     * 会话关闭时候的的动作，需要清理释放资源
     * @param normal
     * @param hint
     */
    void close(boolean normal,String hint);

    ProxyBuffer currentProxyBuffer();

    int sessionId();
    public void writeToChannel() throws IOException;
    public void writeFinished() throws IOException;
    public boolean readFromChannel() throws IOException;
    public void setCallBack(AsynTaskCallBack<T> callBack);
    public void setLastThrowable(Throwable e);
    public boolean hasError();
    public Throwable getLastThrowableAndReset();
    public String getLastThrowableInfoTextAndReset();
}
