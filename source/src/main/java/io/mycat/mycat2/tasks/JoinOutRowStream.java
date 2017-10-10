package io.mycat.mycat2.tasks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.HBT.RowMeta;
import io.mycat.mycat2.HBT.SqlMeta;
import io.mycat.mycat2.beans.MySQLPackageInf;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.mysql.packet.QueryPacket;
import io.mycat.proxy.ProxyBuffer;

public class JoinOutRowStream extends FetchIntoRowStream {
	private static Logger logger = LoggerFactory.getLogger(JoinOutRowStream.class);
    
	public JoinOutRowStream(MySQLSession optSession, SqlMeta sqlMeta, RowMeta rowMeta) {
		super(optSession, sqlMeta,rowMeta);
	}
	
	
	@Override
	void onRsFinish(MySQLSession session, boolean success,String msg) throws IOException {
		if(callBack != null) {
			callBack.finished(session, null, success, null);
		}
        logger.debug("session[{}] load result finish",session);

	}
	
}
