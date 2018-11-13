package io.mycat.mycat2.cmds;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.judge.MySQLProxyStateMHepler;
import io.mycat.mysql.packet.CurrPacketType;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.util.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;

import static io.mycat.mycat2.cmds.LoadDataState.*;
import static io.mycat.util.LoadDataUtil.change2;

/**
 * 进行load data的命令处理
 *
 * @author wuzhihui cjw
 */
public class LoadDataCommand implements MySQLCommand {

    private static final Logger logger = LoggerFactory.getLogger(LoadDataCommand.class);

    /**
     * 透传的实例对象
     */
    public static final LoadDataCommand INSTANCE = new LoadDataCommand();

    /**
     * 结束flag标识
     */
    //private byte[] overFlag = new byte[FLAGLENGTH];
    @Override
    public boolean procssSQL(MycatSession session) throws IOException {
        ProxyBuffer curBuffer = session.proxyBuffer;
        /*
         * 获取后端连接可能涉及到异步处理,这里需要先取消前端读写事件
         */
        session.clearReadWriteOpts();
        session.getBackend((mysqlsession, sender, success, result) -> {
            if (success) {
                session.loadDataStateMachine = CLIENT_2_SERVER_COM_QUERY;
                // 切换buffer 读写状态
                curBuffer.flip();

                curBuffer.readIndex = curBuffer.writeIndex;
                // 读取结束后 改变 owner，对端Session获取，并且感兴趣写事件
                session.giveupOwner(SelectionKey.OP_READ);
                // 进行传输，并检查返回结果检查 ，当传输完成，就将切换为正常的透传
                mysqlsession.writeToChannel();
            }
        });
        return false;
    }


    @Override
    public void clearFrontResouces(MycatSession session, boolean sessionCLosed) {
        // TODO Auto-generated method stub

    }


    @Override
    public void clearBackendResouces(MySQLSession session, boolean sessionCLosed) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean onBackendResponse(MySQLSession session) {
        MycatSession mycatSession = session.getMycatSession();
        try {
            if (mycatSession.loadDataStateMachine == SERVER_2_CLIENT_COM_QUERY_RESPONSE || mycatSession.loadDataStateMachine == SERVER_2_CLIENT_OK_PACKET) {
                boolean readed = session.readFromChannel();
                if (readed) {
                    CurrPacketType currPacketType = mycatSession.resolveMySQLPackage(true, false);
                    if (currPacketType == CurrPacketType.Full) {
                        MySQLProxyStateMHepler.on(session.responseStateMachine,session.curMSQLPackgInf.pkgType, session.proxyBuffer,session);
                        session.setIdle(!session.responseStateMachine.isInteractive());
                        ProxyBuffer proxyBuffer = mycatSession.proxyBuffer;
                        proxyBuffer.flip();
                        mycatSession.takeOwner(SelectionKey.OP_WRITE);
                        mycatSession.writeToChannel();
                    }
                }
                return false;
            }
        } catch (IOException e) {
            logger.info(e.getLocalizedMessage());
            String errmsg = " load data fail in " + mycatSession.loadDataStateMachine;
            mycatSession.sendErrorMsg(ErrorCode.ER_INVALID_DEFAULT, errmsg);
            return true;
        }
        return false;
    }

    @Override
    public boolean onBackendClosed(MySQLSession session, boolean normal) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean onFrontWriteFinished(MycatSession session) throws IOException {
        switch (session.loadDataStateMachine) {
            case SERVER_2_CLIENT_COM_QUERY_RESPONSE:
                session.proxyBuffer.flip();
                change2(session, CLIENT_2_SERVER_CONTENT_FILENAME);
                session.takeOwner(SelectionKey.OP_READ);
                return false;
            case SERVER_2_CLIENT_OK_PACKET:
                session.proxyBuffer.flip();
                session.takeOwner(SelectionKey.OP_READ);
                session.loadDataStateMachine = NOT_LOAD_DATA;
                return false;
            default:
                throw new RuntimeException("unknown state!!!");
        }
    }

    @Override
    public boolean onBackendWriteFinished(MySQLSession session) {
        MycatSession mycatSession = session.getMycatSession();
        switch (mycatSession.loadDataStateMachine) {
            case CLIENT_2_SERVER_COM_QUERY:
                mycatSession.proxyBuffer.flip();
                change2(mycatSession, SERVER_2_CLIENT_COM_QUERY_RESPONSE);
                mycatSession.giveupOwner(SelectionKey.OP_READ);
                break;
            case CLIENT_2_SERVER_CONTENT_FILENAME:
                mycatSession.proxyBuffer.flip();
                mycatSession.takeOwner(SelectionKey.OP_READ);
                break;
            case CLIENT_2_SERVER_EMPTY_PACKET:
                mycatSession.proxyBuffer.flip();
                change2(mycatSession, SERVER_2_CLIENT_OK_PACKET);
                mycatSession.giveupOwner(SelectionKey.OP_READ);
                break;
            default:
        }
        return false;
    }
}
