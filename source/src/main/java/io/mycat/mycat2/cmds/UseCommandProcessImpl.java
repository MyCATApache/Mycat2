package io.mycat.mycat2.cmds;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatConfig;
import io.mycat.mycat2.beans.SchemaBean;
import io.mycat.mycat2.tasks.AbstractBackendIOTask;
import io.mycat.mycat2.tasks.BackendUseDBTask;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.ProxyRuntime;

/**
 * 负责处理Use命令
 * @author zhangwy
 *
 */
public class UseCommandProcessImpl implements SQLComandProcessInf {
    public static final UseCommandProcessImpl INSTANCE=new UseCommandProcessImpl();
    public static final byte[] OK = new byte[]{7, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0};
    private static Logger logger = LoggerFactory.getLogger(UseCommandProcessImpl.class);

    


    /* 
     * @see io.mycat.mycat2.cmds.SQLComandProcessInf#commandProc(io.mycat.mycat2.MySQLSession)
     */
    @Override
    public void commandProc(MySQLSession session) throws IOException {
        ProxyBuffer curBuffer = session.frontBuffer;
        SocketChannel curChannel = session.backendChannel;
        //处理前段的请求com_init 命令
            int offset = curBuffer.readState.optPostion;
            curBuffer.skip(5);
            String schema = curBuffer.readNULString();
            MycatConfig mycatConfig= (MycatConfig)ProxyRuntime.INSTANCE.getProxyConfig();
            SchemaBean schemaBean = ((MycatConfig)ProxyRuntime.INSTANCE.getProxyConfig()).getMycatSchema(schema);
            
            //String replicat = schemaBean.getDefaultDN().getMysqlReplica();
            //mycatConfig.getMySQLReplicatSet(replicat).getLBReadDH();
            if(null == schemaBean) {
                ErrorPacket errPkg = new ErrorPacket();
                errPkg.packetId = 1;
                errPkg.errno  = 1049;
                errPkg.message = "Unknown database 123'" + schema + "'";
                session.frontBuffer.reset();
                session.responseOKOrError(errPkg, true);
                logger.debug(errPkg.message);
            } 
            if(session.backendChannel == null) {
                logger.debug("back connection is null response ok to front");
                session.schema = schemaBean;
                curBuffer.reset();
                session.answerFront(OK);
            } else {
                SchemaBean preSchema = session.schema ;
                session.schema = schemaBean;
                AbstractBackendIOTask task = new BackendUseDBTask(session);
                task.setCallback(( mySession,  sender,  success,  result) -> {
                    if(success) {
                        session.frontBuffer.reset();
                        session.answerFront(OK);
                    } else {
                          session.schema = preSchema;
                          ErrorPacket errPkg = new ErrorPacket();
                          errPkg.packetId = 1;
                          errPkg.errno  = 1049;
                          errPkg.message = "back connection set database '" + ((ErrorPacket)( result)).message;
                          session.frontBuffer.reset();
                          session.responseOKOrError(errPkg, true);
                          session.frontBuffer.reset();
                          logger.debug(errPkg.message);
                    }
             });
        } 
    }


	

	


}
