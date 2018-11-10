package io.mycat.mycat2.cmds;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.beans.MySQLPackageInf;
import io.mycat.mycat2.console.SessionKey;
import io.mycat.mysql.packet.CurrPacketType;
import io.mycat.proxy.ProxyBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;

/**
 * 进行load data的命令处理
 *
 * @author wuzhihui
 */
public class LoadDataCommand implements MySQLCommand {

    private static final Logger logger = LoggerFactory.getLogger(LoadDataCommand.class);

    /**
     * 透传的实例对象
     */
    public static final LoadDataCommand INSTANCE = new LoadDataCommand();

    /**
     * loaddata传送结束标识长度
     */
    private static final int FLAGLENGTH = 4;

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
                mysqlsession.setCurNIOHandler(LoadDataStream.CLIENT_2_SERVER_COM_QUERY);
                session.setCurNIOHandler(LoadDataStream.CLIENT_2_SERVER_COM_QUERY);
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


        try {
            if (session.readFromChannel()) {
                CurrPacketType currPacketType = session.resolveMySQLPackage();
                if (currPacketType == CurrPacketType.Full) {
                    MySQLPackageInf curMSQLPackgInf = session.curMSQLPackgInf;
                    if (curMSQLPackgInf.isOkPacket()) {
                        session.getMycatSession().getAttrMap().put(SessionKey.LOAD_DATA_FINISH_KEY, Boolean.TRUE);
                    }
                    MycatSession mycatSession = session.getMycatSession();
                    mycatSession.proxyBuffer.flip();
                    mycatSession.takeOwner(SelectionKey.OP_WRITE);
                    mycatSession.writeToChannel();
                }
            }

        } catch (Exception e) {

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
//		//向前端写完数据，前段进入读状态
//        if (session.getAttrMap().get(SessionKey.LOAD_DATA_FINISH_KEY) == Boolean.TRUE){
//
//            session.proxyBuffer.flip();
//            session.change2ReadOpts();
//            session.curSQLCommand =DirectPassthrouhCmd.INSTANCE;
//            return true;
//        }else {
//            session.proxyBuffer.flip();
//            session.takeOwner(SelectionKey.OP_READ);
//            session
//            return false;
//        }
        return false;
    }

    @Override
    public boolean onBackendWriteFinished(MySQLSession session) {
        session.getMycatSession().giveupOwner(SelectionKey.OP_READ);
        session.proxyBuffer.flip();
        return false;
    }
}
