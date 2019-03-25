package io.mycat.proxy;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MySQLSessionManager;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.beans.MySQLMetaBean;
import io.mycat.mycat2.beans.MySQLRepBean;
import io.mycat.mycat2.beans.conf.DNBean;
import io.mycat.mycat2.net.MainMySQLNIOHandler;
import io.mycat.mycat2.net.MainMycatNIOHandler;
import io.mycat.mycat2.tasks.AsynTaskCallBack;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.proxy.buffer.BufferPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * mycat 多个Session会话
 *
 * @author yanjunli
 * @author Leader us
 */
public class MycatReactorThread extends ProxyReactorThread<MycatSession> {

    protected final static Logger logger = LoggerFactory.getLogger(MycatReactorThread.class);

    /**
     * 每个Reactor独立的一个后端连接池不共享（在MySQLSessionManager中记录），每个Reactor线程自己负责清理释放多余富足的连接，确保
     * 所有Reactor线程拥有的后端连接数总和为全局连接池总数
     */
    public final MySQLSessionManager mysqlSessionMan = new MySQLSessionManager();

    public MycatReactorThread(BufferPool bufPool) throws IOException {
        super(bufPool);
    }

    /**
     * 从当前reactor中获取连接 3. reactor thread中空闲的backend 4. 连接池中的 backend 5. 是否可以新建连接
     *
     * @return
     * @throws IOException
     */
    public void tryGetMySQLAndExecute(MycatSession session, AsynTaskCallBack<MySQLSession> callback) throws IOException {
        if (Thread.currentThread() != this) {
            throw new RuntimeException("Not in current MycatReactorThread");
        }
        if (session.getTargetDataNode() == null) {
            logger.warn("{} not specified SQL target DataNode ,so set to default dataNode ", session);
            DNBean targetDataNode = ProxyRuntime.INSTANCE.getConfig().getMycatDataNodeMap()
                    .get(session.getMycatSchema().getDefaultDataNode());
            session.setTargetDataNode(targetDataNode);
        }
        MySQLRepBean targetRepBean = ProxyRuntime.INSTANCE.getConfig().getMySQLRepBean(session.getTargetDataNode().getReplica());
        MySQLMetaBean targetMetaBean = targetRepBean.getBalanceMetaBean(false);
        if (targetMetaBean  == null){
            throw new RuntimeException("没有可用的副本!!!");
        }
        logger.debug("tryGetMySQLAndExecute  on DataNode {} for callback {} ", session.getTargetDataNode(), callback);
        MySQLSession curBackend = session.getCurBackend();
        if (curBackend != null) {
            mysqlSessionMan.removeSession(curBackend);
            if (curBackend.synchronizedState(session.getTargetDataNode().getDatabase())) {
                callback.finished(curBackend, Thread.currentThread(), true, null);
            } else {
                // 同步数据库连接状态后回调
                AsynTaskCallBack<MySQLSession> callback2 = (mysqlsession, sender, success, result) -> {
                    logger.debug("同步数据库连接状态后回调");
                    if (success) {
                        callback.finished(curBackend, Thread.currentThread(), true, null);
                    } else {
                        session.closeAllBackendsAndResponseError(success, ((ErrorPacket) result));
                    }
                };
                curBackend.syncAndCallback(callback2);
            }
        } else {
            // 从ds中获取已经建立的连接
            List<MySQLSession> mySQLSessionList = mysqlSessionMan.getSessionsOfHost(targetMetaBean);
            if (mySQLSessionList != null && !mySQLSessionList.isEmpty()) {
                for (MySQLSession mysqlSession : mySQLSessionList) {
                    if (mysqlSession.isIdle()) {
                        logger.debug("Using the existing session in the datasource  for {}:{}",
                                mysqlSession.getMySQLMetaBean().getDsMetaBean().getIp(),
                                mysqlSession.getMySQLMetaBean().getDsMetaBean().getPort());
                        MycatSession oldMycatSession = mysqlSession.getMycatSession();
                        if (oldMycatSession != null) {
                            throw new RuntimeException("Don't snatch other other's sessions!" + oldMycatSession.toString());
                        }
                        session.bindBackend(mysqlSession);
                        mysqlSessionMan.removeSession(mysqlSession);
                        mysqlSession.syncAndCallback(callback);
                        return;
                    }
                }
            }

            // 新建连接
            logger.info("create new connection  ");
            mysqlSessionMan.createMySQLSession(targetMetaBean, session.getMycatSchema(),
                    (optSession, Sender, exeSucces, retVal) -> {
                        // 恢复默认的Handler
                        session.setCurNIOHandler(MainMycatNIOHandler.INSTANCE);
                        if (exeSucces) {
                            optSession.setCurNIOHandler(MainMySQLNIOHandler.INSTANCE);
                            session.bindBackend(optSession);
                            optSession.syncAndCallback(callback);
                            // addMySQLSession(targetMetaBean, optSession);
                        } else {
                            callback.finished(optSession, Sender, exeSucces, retVal);
                        }
                    });
        }

    }

    /**
     * 用于心跳 时，获取可用连接
     *
     * @param mySQLMetaBean
     * @param callback
     * @throws IOException
     */
    public void getHeatBeatCon(MySQLMetaBean mySQLMetaBean, AsynTaskCallBack<MySQLSession> callback)
            throws IOException {
        if (Thread.currentThread() != this) {
            throw new RuntimeException("Not in current MycatReactorThread");
        }
        // 从ds中获取已经建立的连接
        List<MySQLSession> mySQLSessionList = this.mysqlSessionMan.getSessionsOfHost(mySQLMetaBean);
        if (mySQLSessionList != null && !mySQLSessionList.isEmpty()) {
            for (MySQLSession mysqlSession : mySQLSessionList) {
                if (mysqlSession.isIdle()&&!mysqlSession.isClosed()) {//可能会获得到未开启连接的session
                    logger.debug("Using the existing session in the datasource  for heart beat. {}:{}",
                            mysqlSession.getMySQLMetaBean().getDsMetaBean().getIp(),
                            mysqlSession.getMySQLMetaBean().getDsMetaBean().getPort());
                    if (mysqlSession.getMycatSession() != null) {
                        throw new RuntimeException("Illegal state");
                    }
                    mysqlSession.setIdle(false);
                    callback.finished(mysqlSession, null, true, null);
                    return;
                }
            }
        }

        // 新建连接
        logger.info("create new connection {} ", mySQLMetaBean);
        mysqlSessionMan.createMySQLSession(mySQLMetaBean, null, (optSession, Sender, exeSucces, retVal) -> {
            if (exeSucces) {
                // 恢复默认的Handler
                optSession.setCurNIOHandler(MainMySQLNIOHandler.INSTANCE);
                callback.finished(optSession, null, true, null);
            } else {
                callback.finished(optSession, null, false, retVal);
            }
        });
    }

}
