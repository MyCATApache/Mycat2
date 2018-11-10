package io.mycat.mycat2.tasks;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.mysql.packet.QueryPacket;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.util.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;

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
    }

    public void syncState(MycatSession mycatSession,MySQLSession mySQLSession) throws IOException {
        ProxyBuffer proxyBuf = mySQLSession.proxyBuffer;
        proxyBuf.reset();
        QueryPacket queryPacket = new QueryPacket();
        queryPacket.packetId = 0;
        
        queryPacket.sql = "";
        if(!mySQLSession.getMySQLMetaBean().isSlaveNode()){
        	//隔离级别同步
        	if(mycatSession.isolation != mySQLSession.isolation){
                queryPacket.sql += mycatSession.isolation.getCmd();
                syncCmdNum++;
            }
            //提交方式同步
            if(mycatSession.autoCommit != mySQLSession.autoCommit){
                queryPacket.sql += mycatSession.autoCommit.getCmd();
                syncCmdNum++;
            }
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
        	logger.debug("synchronzation state [{}]to bakcend.session={}",queryPacket.sql,mySQLSession.toString());
            queryPacket.write(proxyBuf);
            proxyBuf.flip();
            proxyBuf.readIndex = proxyBuf.writeIndex;
            try {
            	session.writeToChannel();
			}catch(ClosedChannelException e){
				logger.debug("synchronzation state task end ");
				if(session.getMycatSession()!=null){
					session.close(false, "backend connection is closed!");
				}
				session.close(false, e.getMessage());
				return;
			} catch (Exception e) {
				String errmsg = "backend state sync Error. " + e.getMessage();
				errPkg = new ErrorPacket();
				errPkg.packetId = 1;
				errPkg.errno = ErrorCode.ER_UNKNOWN_ERROR;
				errPkg.message = errmsg;
				logger.error(errmsg);
				e.printStackTrace();
				this.finished(false);
				
			}
        }else{
        	finished(true);
        }
    }
    
    public int getSyncCmdNum(){
    	return syncCmdNum;
    }

    @Override
    public void onSocketRead(MySQLSession session) throws IOException {
        session.proxyBuffer.reset();        
		try {
    		if (!session.readFromChannel()){
    			return;
    		}
		}catch(ClosedChannelException e){
			session.close(false, e.getMessage());
			return;
		}catch (IOException e) {
			logger.error("the backend synchronzation task Error. {}",e.getMessage());
			e.printStackTrace();
			this.finished(false);
			return;
		}
        
        boolean isAllOK = true;
        while (syncCmdNum >0) {
        	switch (session.resolveMySQLPackage(true)) {
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
            logger.debug("synchronzation state task end ");
            finished(true);
        } else {
            errPkg = new ErrorPacket();
            errPkg.read(session.proxyBuffer);
            logger.error("backend state sync Error.Err No. " + errPkg.errno + "," + errPkg.message);
            finished(false);
        }
    }
}
