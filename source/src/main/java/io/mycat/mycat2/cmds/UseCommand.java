package io.mycat.mycat2.cmds;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatConfig;
import io.mycat.mycat2.SQLCommand;
import io.mycat.mycat2.beans.SchemaBean;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.mysql.packet.INITDBPacket;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.ProxyRuntime;

/**
 * 负责处理Show命令
 * @author wuzhihui
 *
 */
public class UseCommand implements SQLCommand{
    public static final UseCommand INSTANCE=new UseCommand();
    public static final byte[] OK = new byte[]{7, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0};
    private static Logger logger = LoggerFactory.getLogger(UseCommand.class);

	@Override
	public boolean procssSQL(MySQLSession session, boolean backresReceived) throws IOException  {
	    ProxyBuffer curBuffer = session.frontBuffer;
        SocketChannel curChannel = session.backendChannel;
        //处理前段的请求com_init 命令
        if(backresReceived == false) {
            int offset = curBuffer.readState.optPostion;
            curBuffer.skip(5);
            String schema = curBuffer.readNULString();
            SchemaBean schemaBean = ((MycatConfig)ProxyRuntime.INSTANCE.getProxyConfig()).getMycatSchema(schema);
            if(null == schemaBean) {
                ErrorPacket errPkg = new ErrorPacket();
                errPkg.packetId = 1;
                errPkg.errno  = 1049;
                errPkg.message = "Unknown database 123'" + schema + "'";
                session.frontBuffer.reset();
                session.responseOKOrError(errPkg, true);
                logger.debug(errPkg.message);
                session.curSQLCommand = DirectPassthrouhCmd.INSTANCE;
                return true;
            } 
//            if(session.backendChannel == null) {
                logger.debug("back connection is null response ok to front");
                curBuffer.reset();
                session.answerFront(OK);
                session.curSQLCommand = DirectPassthrouhCmd.INSTANCE;
                return true;
//            } else {
//                INITDBPacket initDBPacket = new INITDBPacket();
//                initDBPacket.sql = "use " + schemaBean.getDefaultDN().getDatabase();
//                ProxyBuffer buffer = session.frontBuffer;
//                buffer.reset();
//                buffer.changeOwner(false);
//                initDBPacket.write(buffer);
//                buffer.flip();
//                session.writeToChannel(buffer, session.backendChannel);
//                session.modifySelectKey();
//                return false;
//            }
        } 
        if (backresReceived) {// 收到后端发来的报文
            curBuffer = session.frontBuffer;
            curChannel = session.frontChannel;
            //成功 或者失败
           
            if(session.curFrontMSQLPackgInf.pkgType == MySQLPacket.OK_PACKET) {
                logger.debug("success set back connnection database, response ok to front");
                session.frontBuffer.reset();
                session.answerFront(OK);
                session.curSQLCommand = DirectPassthrouhCmd.INSTANCE;
                return true;
            } else {
                ErrorPacket errPkg = new ErrorPacket();
                errPkg.packetId = 1;
                errPkg.errno  = 1049;
                errPkg.message = "back connection set database '";
                curBuffer.changeOwner(true);
                session.frontBuffer.reset();
                session.responseOKOrError(errPkg, true);
                logger.debug(errPkg.message);
                curBuffer.changeOwner(true);
                session.curSQLCommand = DirectPassthrouhCmd.INSTANCE;
                session.modifySelectKey();
           
            }
        }

        return true;
	}

	
	@Override
	public void clearResouces(boolean sessionCLosed) {
		// TODO Auto-generated method stub
		
	}

}
