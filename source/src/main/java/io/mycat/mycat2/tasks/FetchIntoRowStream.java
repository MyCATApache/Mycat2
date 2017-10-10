package io.mycat.mycat2.tasks;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.HBT.RowMeta;
import io.mycat.mycat2.HBT.SqlMeta;
import io.mycat.mycat2.beans.MySQLPackageInf;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.mysql.packet.QueryPacket;
import io.mycat.proxy.ProxyBuffer;

public class FetchIntoRowStream extends BackendIOTaskWithResultSet<MySQLSession> {
	private static Logger logger = LoggerFactory.getLogger(FetchIntoRowStream.class);

	protected SqlMeta sqlMeta;
	protected RowMeta rowMeta;
	//protected MySQLSession optSession;
    
	public FetchIntoRowStream(MySQLSession optSession, SqlMeta sqlMeta, RowMeta rowMeta) {
		this.useNewBuffer = true;
		this.sqlMeta = sqlMeta;
		this.rowMeta = rowMeta;
		setSession(optSession, true, false);
		this.session = optSession;
		//this.session = optSession;
//		session.getMycatSession().setCurNIOHandler(this);
	}
	public void fetchStream() {
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
	@Override
	void onRsColCount(MySQLSession session) {
		ProxyBuffer proxyBuffer = session.proxyBuffer;
		MySQLPackageInf curMSQLPackgInf = session.curMSQLPackgInf;
	    int fieldCount = (int) proxyBuffer.getLenencInt(curMSQLPackgInf.startPos + MySQLPacket.packetHeaderSize);
	    
	    rowMeta.init(fieldCount);
	}

	@Override
	void onRsColDef(MySQLSession session) {
		ProxyBuffer proxyBuffer = session.proxyBuffer;
        MySQLPackageInf curMQLPackgInf = session.curMSQLPackgInf;

        int tmpReadIndex = proxyBuffer.readIndex;
        int rowDataIndex = curMQLPackgInf.startPos+MySQLPacket.packetHeaderSize;
        proxyBuffer.readIndex = rowDataIndex;
        proxyBuffer.readLenencString();  //catalog
        proxyBuffer.readLenencString();  //schema 
        proxyBuffer.readLenencString();  //table
        proxyBuffer.readLenencString();  //orgTable
        String name     = proxyBuffer.readLenencString();  //name
        proxyBuffer.readLenencString();
        proxyBuffer.readBytes(7); // 1(filler) + 2(charsetNumber) + 4 (length)
		byte fieldType = proxyBuffer.readByte();
        rowMeta.getHeaderResultSet().addFiled(name, fieldType);
//        byte[] row = proxyBuffer.getBytes(rowDataIndex,
//        		tmpReadIndex - rowDataIndex);
//		rowMeta.addField(row , name);
        proxyBuffer.readIndex = tmpReadIndex;
	}

	@Override
	void onRsRow(MySQLSession session) {
		ProxyBuffer proxyBuffer = session.proxyBuffer;
        MySQLPackageInf curMQLPackgInf = session.curMSQLPackgInf;
        int rowDataIndex = curMQLPackgInf.startPos + MySQLPacket.packetHeaderSize;
        int fieldCount = rowMeta.fieldCount;
        int tmpReadIndex = proxyBuffer.readIndex;
        proxyBuffer.readIndex = rowDataIndex;
        ArrayList<byte[]> row = new ArrayList<byte[]>(fieldCount);
		for(int i = 0; i < fieldCount; i++) {
			row.add(proxyBuffer.readLenencBytes());
		}
		rowMeta.addFieldValues(row);
		proxyBuffer.readIndex = tmpReadIndex;
	}

	@Override
	void onRsFinish(MySQLSession session, boolean success, String msg) throws IOException {
		if(callBack != null) {
			if(success == false) {
				this.errPkg = new ErrorPacket();
		        MySQLPackageInf curMQLPackgInf = session.curMSQLPackgInf;
		        session.proxyBuffer.readIndex = curMQLPackgInf.startPos;
				this.errPkg.read(session.proxyBuffer);
				revertPreBuffer();
		        callBack.finished(session, this, success, this.errPkg);
			} else {
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
	public RowMeta getRowMeta() {
		return rowMeta;
	}
	public void setRowMeta(RowMeta rowMeta) {
		this.rowMeta = rowMeta;
	}
	public MySQLSession getOptSession() {
		return session;
	}
	public void setOptSession(MySQLSession optSession) {
		this.session = optSession;
	}
	
}
