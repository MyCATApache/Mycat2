package io.mycat.mycat2.tasks;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.beans.MySQLPackageInf;
import io.mycat.mycat2.hbt.MyFunction;
import io.mycat.mycat2.hbt.ResultSetMeta;
import io.mycat.mycat2.hbt.SqlMeta;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.mysql.packet.QueryPacket;
import io.mycat.proxy.ProxyBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class RowStream extends BackendIOTaskWithResultSet<MySQLSession> {
	private static Logger logger = LoggerFactory.getLogger(RowStream.class);

	protected SqlMeta sqlMeta;
//	protected RowMeta rowMeta;
    Function<ResultSetMeta, ResultSetMeta> onHeader ;
    Function<List<byte[]>, List<byte[]>> onRowData ;
    MyFunction onEnd ;

    private ResultSetMeta resultSetMeta;
    
	public RowStream(MySQLSession optSession, SqlMeta sqlMeta,
	        Function<ResultSetMeta, ResultSetMeta> onHeader,Function<List<byte[]>, List<byte[]>> onRowData, MyFunction onEnd) {
		this.useNewBuffer = true;
		this.sqlMeta = sqlMeta;
		setSession(optSession, true, false);
		this.session = optSession;
		this.onHeader = onHeader;
		this.onRowData = onRowData;
		this.onEnd = onEnd;
	}
	public void fetchStream() {
		/*设置为忙*/
		session.setIdle(false);
        ProxyBuffer proxyBuf = session.proxyBuffer;
        proxyBuf.reset();
        QueryPacket queryPacket = new QueryPacket();
        queryPacket.packetId = 0;
        queryPacket.sql = sqlMeta.sql;
        queryPacket.write(proxyBuf);
        session.setCurNIOHandler(this);
        proxyBuf.flip();
        proxyBuf.readIndex = proxyBuf.writeIndex;
        try {
			this.session.writeToChannel();
		} catch (IOException e) {
			logger.error(" The FetchIntoRowStream  task write  is error . {}",e.getMessage());
			e.printStackTrace();
		}
	}
	public void fetchStream(ProxyBuffer proxyBuf) {
		/*设置为忙*/
		session.setIdle(false);
		session.setCurNIOHandler(this);
		proxyBuf.flip();
		proxyBuf.readIndex = proxyBuf.writeIndex;
		try {
			this.session.writeToChannel();
		} catch (IOException e) {
			logger.error(" The FetchIntoRowStream  task write  is error . {}",e.getMessage());
			e.printStackTrace();
		}
	}
	@Override
	void onRsColCount(MySQLSession session) {
		ProxyBuffer proxyBuffer = session.proxyBuffer;
		MySQLPackageInf curMSQLPackgInf = session.curMSQLPackgInf;
	    int fieldCount = (int) proxyBuffer.getLenencInt(curMSQLPackgInf.startPos + MySQLPacket.packetHeaderSize);
	    
	    this.resultSetMeta = new ResultSetMeta(fieldCount);
	}

	@Override
	void onRsColDef(MySQLSession session) {
		ProxyBuffer proxyBuffer = session.proxyBuffer;
        MySQLPackageInf curMQLPackgInf = session.curMSQLPackgInf;

        int tmpReadIndex = proxyBuffer.readIndex;
        int rowDataIndex = curMQLPackgInf.startPos+MySQLPacket.packetHeaderSize;
        proxyBuffer.readIndex = rowDataIndex;
        proxyBuffer.readLenencString();  //catalog
		proxyBuffer.readLenencString();  //mycatSchema
        proxyBuffer.readLenencString();  //table
        proxyBuffer.readLenencString();  //orgTable
        String name     = proxyBuffer.readLenencString();  //name
        proxyBuffer.readLenencString();
        proxyBuffer.readBytes(7); // 1(filler) + 2(charsetNumber) + 4 (length)
		int fieldType = proxyBuffer.readByte() & 0xff;
		this.resultSetMeta.addField(name, fieldType);
        proxyBuffer.readIndex = tmpReadIndex;
        if(resultSetMeta.getFiledCount() == resultSetMeta.getRealFieldNameListSize()) {
            this.onHeader.apply(resultSetMeta);
        }
	}

	@Override
	void onRsRow(MySQLSession session) {
		ProxyBuffer proxyBuffer = session.proxyBuffer;
        MySQLPackageInf curMQLPackgInf = session.curMSQLPackgInf;
        int rowDataIndex = curMQLPackgInf.startPos + MySQLPacket.packetHeaderSize;
        int fieldCount = resultSetMeta.getFiledCount();
        int tmpReadIndex = proxyBuffer.readIndex;
        proxyBuffer.readIndex = rowDataIndex;
        ArrayList<byte[]> row = new ArrayList<byte[]>(fieldCount);
		for(int i = 0; i < fieldCount; i++) {
			row.add(proxyBuffer.readLenencBytes());
		}
        proxyBuffer.readIndex = tmpReadIndex;
		onRowData.apply(row);
	}

	@Override
	void onRsFinish(MySQLSession session, boolean success, String msg) throws IOException {
		if(callBack != null) {
			if(success == false) {
				this.errPkg = new ErrorPacket();
		        MySQLPackageInf curMQLPackgInf = session.curMSQLPackgInf;
		        session.proxyBuffer.readIndex = curMQLPackgInf.startPos;
				this.errPkg.read(session.proxyBuffer);
				session.setIdle(true);
				revertPreBuffer();
		        callBack.finished(session, this, success, this.errPkg);
			} else {
				session.setIdle(true);
				revertPreBuffer();
				callBack.finished(session, null, success, null);
			}
		}
        logger.debug("session[{}] load result finish",session);

	}
	
	public SqlMeta getSqlMeta() {
		return sqlMeta;
	}
	public void setSqlMeta(SqlMeta sqlMeta) {
		this.sqlMeta = sqlMeta;
	}

	public MySQLSession getOptSession() {
		return session;
	}
	public void setOptSession(MySQLSession optSession) {
		this.session = optSession;
	}
	
}
