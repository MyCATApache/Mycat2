package io.mycat.mycat2.Interceptor.impl;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MyCommand;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.Interceptor.Interceptor;
import io.mycat.mycat2.net.DefaultMycatSessionHandler;

/**
 *  设置后端的执行sql
 * @author zwy
 */
public class DefaultIntercepor  implements Interceptor {
	private static Logger logger = LoggerFactory.getLogger(DefaultMycatSessionHandler.class);
	
	public static final Interceptor INSTANCE = new DefaultIntercepor();
	public boolean intercept(MycatSession mycatSession) throws IOException {
		MyCommand myCommand = mycatSession.getMyCommand();
		if(myCommand!=null){
			mycatSession.putSQLCmd(this, (MyCommand) myCommand);
			mycatSession.curSQLCommand = myCommand;
		}else{
			logger.error(" current packageTyps is not support,please fix it!!! the packageType is {} ",mycatSession.curMSQLPackgInf);
		}
		return false;
	}

}
