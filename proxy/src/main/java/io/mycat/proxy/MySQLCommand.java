package io.mycat.proxy;

import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.MySQLSession;

import java.io.IOException;

public interface MySQLCommand {
    /**
     * 收到后端应答报文
     *
     * @param session 后端MySQLSession
     * @return 是否完成了应答
     * @throws IOException
     */
    public default boolean onBackendResponse(MySQLSession session) throws IOException {
        return true;
    }

    /**
     * 后端连接关闭了
     *
     * @param session
     * @param normal
     * @return 是否完成了应答
     * @throws IOException
     */
    public default boolean onBackendClosed(MySQLSession session, boolean normal) throws IOException {
        return true;
    }

    /**
     * 前端数据已经写完，即发送给MyCat前端应答的数据（结果集等）已经写完到Channel（可做流控使用）
     *
     * @param session
     * @return 是否完成了应答
     * @throws IOException
     */
    public boolean onFrontWriteFinished(MycatSession session) throws IOException;

    /**
     * 后端数据已经写完，即发送给MySQL的数据（SQL等）已经写完到Channel（可做流控使用）
     *
     * @param session
     * @return 是否完成了应答
     * @throws IOException
     */
    public default boolean onBackendWriteFinished(MySQLSession session) throws IOException{
        return true;
    }

    /**
     * 清理资源，只清理自己产生的资源（如创建了Buffer，以及Session中放入了某些对象）
     *
     * @param sessionCLosed 是否因为Session关闭而清理资源，此时应该彻底清理
     */
    public void clearResouces(MycatSession session, boolean sessionCLosed);
   public void clearResouces(MySQLSession session, boolean sessionCLosed);
    /**
     * 直接应答请求报文，如果是直接应答的，则此方法调用一次就完成了，如果是靠后端响应后才应答，则至少会调用两次，
     * 是否完成了应答的标准：发给前端的报文已经全部写出
     *
     * @param session
     * @return 是否完成了应答（如果是多个阶段应答的，则只在最后一个节点才返回true表示应答完成）
     */
    public boolean procssSQL(MycatSession session) throws IOException;

}
