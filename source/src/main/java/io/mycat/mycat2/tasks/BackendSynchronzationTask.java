package io.mycat.mycat2.tasks;

import java.io.IOException;

import io.mycat.mycat2.MycatSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mysql.packet.CommandPacket;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.mysql.packet.QueryPacket;
import io.mycat.proxy.ProxyBuffer;

/**
 * Created by ynfeng on 2017/8/13.
 * <p>
 * 同步状态至后端数据库，包括：字符集，事务，隔离级别等
 */
public class BackendSynchronzationTask extends AbstractBackendIOTask<MySQLSession> {
    private static Logger logger = LoggerFactory.getLogger(BackendSynchronzationTask.class);

    private int syncCmdNum = 0;
    private MycatSession mycatSession;

    public BackendSynchronzationTask(MycatSession mycatSession,MySQLSession mySQLSession) throws IOException {
        super(mySQLSession,true);
        this.mycatSession = mycatSession;
        syncState(mycatSession,mySQLSession);
    }

    private void syncState(MycatSession mycatSession,MySQLSession mySQLSession) throws IOException {
        logger.info("synchronzation state to bakcend.session=" + mySQLSession.toString());
        ProxyBuffer proxyBuf = mySQLSession.proxyBuffer;
        proxyBuf.reset();
        QueryPacket queryPacket = new QueryPacket();
        queryPacket.packetId = 0;
        //隔离级别同步
        queryPacket.sql = "";
        if(mycatSession.isolation != mySQLSession.isolation){
            queryPacket.sql += mycatSession.isolation.getCmd();
            syncCmdNum++;
        }
        //提交方式同步
        if(mycatSession.autoCommit != mySQLSession.autoCommit){
            queryPacket.sql += mycatSession.autoCommit.getCmd();
            syncCmdNum++;
        }
        //字符集同步
        if (mycatSession.charSet.charsetIndex != mySQLSession.charSet.charsetIndex) {
            //字符集同步,直接取主节点的字符集映射
            //1.因为主节点必定存在
            //2.从节点和主节点的mysql版本号必定一致
            //3.所以直接取主节点
            String charsetName = mySQLSession.getMySQLMetaBean().INDEX_TO_CHARSET.get(mycatSession.charSet.charsetIndex);
            queryPacket.sql += "SET names " + charsetName + ";";
            syncCmdNum++;
        }
        if (syncCmdNum > 0) {
            queryPacket.write(proxyBuf);
            proxyBuf.flip();
            proxyBuf.readIndex = proxyBuf.writeIndex;
            session.writeToChannel();
        }
    }

    @Override
    public void onSocketRead(MySQLSession session) throws IOException {
        session.proxyBuffer.reset();
        if (!session.readFromChannel()) {// 没有读到数据或者报文不完整
            return;
        }
        boolean isAllOK = true;
        while (syncCmdNum >0) {
        	switch (session.resolveMySQLPackage(session.proxyBuffer, session.curMSQLPackgInf, true)) {
			case Full:
				if(session.curMSQLPackgInf.pkgType == MySQLPacket.ERROR_PACKET){
					isAllOK = false;
					syncCmdNum = 0;
				}
				break;
			default:
				return;
        	}
        	syncCmdNum --;
        }

        if (isAllOK) {
            session.autoCommit = mycatSession.autoCommit;
            session.isolation = mycatSession.isolation;
            session.charSet.charsetIndex = mycatSession.charSet.charsetIndex;
            finished(true);
        } else {
            errPkg = new ErrorPacket();
            errPkg.read(session.proxyBuffer);
            logger.warn("backend state sync Error.Err No. " + errPkg.errno + "," + errPkg.message);
            finished(false);
        }
    }
}
