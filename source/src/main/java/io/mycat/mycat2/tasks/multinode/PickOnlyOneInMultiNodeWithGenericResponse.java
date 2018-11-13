package io.mycat.mycat2.tasks.multinode;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.net.MainMySQLNIOHandler;
import io.mycat.mycat2.route.RouteResultset;
import io.mycat.mycat2.tasks.BackendIOTaskWithGenericResponse;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.mysql.packet.OKPacket;
import io.mycat.proxy.ProxyBuffer;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * 
 * <b><code>PickOnlyOneInMultiNodeWithGenericResponse</code></b>
 * <p>
 * 多节点下只选择其中一个节点的返回结果。 例如针对全局表的DML语句，在成功的情况下，只需返回其中一个OK_Packet。
 * </p>
 * <b>Creation Time:</b> 2018-04-28
 * 
 * @author <a href="mailto:flysqrlboy@gmail.com">zhangsiwei</a>
 * @since 2.0
 */
public class PickOnlyOneInMultiNodeWithGenericResponse extends BackendIOTaskWithGenericResponse {

    private static Logger LOGGER = Logger.getLogger(PickOnlyOneInMultiNodeWithGenericResponse.class);
    private RouteResultset routeResultset;

    public PickOnlyOneInMultiNodeWithGenericResponse(MySQLSession session, RouteResultset rrs) {

        /*
         * useNewBuffer为true，表示mysqlsession使用自己的proxyBuffer，
         * 而没有与mycatsession共用proxyBuffer
         */
        this.setSession(session, true);
        this.routeResultset = rrs;
    }

    @Override
    public void onOkResponse(MySQLSession session) {

        routeResultset.countDown(session, () -> {
            try {
                OKPacket okpkg = new OKPacket();
                okpkg.read(session.proxyBuffer);

                ProxyBuffer proxyBuffer = session.getMycatSession().proxyBuffer;
                proxyBuffer.reset();
                okpkg.write(proxyBuffer);
                proxyBuffer.flip();
                proxyBuffer.readIndex = proxyBuffer.writeIndex;
                session.getMycatSession().takeBufferOwnerOnly();
                session.getMycatSession().writeToChannel();
            } catch (IOException ex) {
                LOGGER.error(ex);
            }

        });
    }

    @Override
    public void onErrResponse(MySQLSession session) {
        ErrorPacket errorPacket = new ErrorPacket();
        errorPacket.read(session.getProxyBuffer());
        try {
            session.getMycatSession().closeBackendAndResponseError(session, false, errorPacket);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // TODO 待实现，收到Error packet响应后应该执行回滚（分布式事务）
    }

    @Override
    public void onFinished(boolean success, MySQLSession session) {
        // 恢复默认的Handler
        session.setCurNIOHandler(MainMySQLNIOHandler.INSTANCE);
        // 把mysqlsession的proxybuffer切换回原来的共享buffer，即与mycatSession共享的buffer
        revertPreBuffer();
        session.setIdle(true);
    }
}
