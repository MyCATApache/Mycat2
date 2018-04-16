package io.mycat.mycat2.tasks;

import io.mycat.mycat2.ColumnMeta;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.beans.MySQLPackageInf;
import io.mycat.mycat2.console.SessionKeyEnum;
import io.mycat.mycat2.hbt.MyFunction;
import io.mycat.mycat2.hbt.ResultSetMeta;
import io.mycat.mycat2.hbt.SqlMeta;
import io.mycat.mycat2.net.DefaultMycatSessionHandler;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.mysql.packet.QueryPacket;
import io.mycat.proxy.ProxyBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class MyRowStream extends BackendIOTaskWithResultSet<MySQLSession> {
	private static Logger logger = LoggerFactory.getLogger(MyRowStream.class);
	AbstractDataNodeMerge merge;
	Map<String, ColumnMeta> columToIndx = new HashMap<>();
    private ResultSetMeta resultSetMeta;
	int fieldCount = 0;
	int getFieldCount = 0;
	public MyRowStream(MySQLSession optSession) {
		this.useNewBuffer = true;
		setSession(optSession, true, false);
		this.session = optSession;
	}

	public AbstractDataNodeMerge getAbstractDataNodeMerge() {
		return merge;
	}

	public void setAbstractDataNodeMerge(AbstractDataNodeMerge abstractDataNodeMerge) {
		this.merge = abstractDataNodeMerge;
	}

	//	public void fetchStream() {
//		/*设置为忙*/
//		session.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_CONN_IDLE_FLAG.getKey(), false);
//        ProxyBuffer proxyBuf = session.proxyBuffer;
//        session.setCurNIOHandler(this);
//        proxyBuf.flip();
//        proxyBuf.readIndex = proxyBuf.writeIndex;
//        try {
//			this.session.writeToChannel();
//		} catch (IOException e) {
//			logger.error(" The FetchIntoRowStream  task write  is error . {}",e.getMessage());
//			e.printStackTrace();
//		}
//	}
public void fetchStream(String sql) {
	/*设置为忙*/
	session.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_CONN_IDLE_FLAG.getKey(), false);
	ProxyBuffer proxyBuf = session.proxyBuffer;
	proxyBuf.reset();
	QueryPacket queryPacket = new QueryPacket();
	queryPacket.packetId = 0;
	queryPacket.sql = sql;
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
	public void fetchStream(MycatSession mycatSession) {
		// 切换 buffer 读写状态
		ProxyBuffer proxyBuf = mycatSession.proxyBuffer;
		proxyBuf.flip();
		// 改变 owner，对端Session获取，并且感兴趣写事件
		mycatSession.clearReadWriteOpts();
		/*设置为忙*/
		this.session.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_CONN_IDLE_FLAG.getKey(), false);
		this.session.setCurNIOHandler(this);
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
	    this.fieldCount = fieldCount;
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
		int fieldType = proxyBuffer.readByte() & 0xff;
		this.resultSetMeta.addFiled(name, fieldType);
        proxyBuffer.readIndex = tmpReadIndex;
        if(resultSetMeta.getFiledCount() == resultSetMeta.getRealFieldNameListSize()) {

        }

		columToIndx.put(name, new ColumnMeta(getFieldCount++, fieldType));
		if (fieldCount == getFieldCount) {
			merge.onRowMetaData(columToIndx, fieldCount);
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
		ByteBuffer byteBuffer = ByteBuffer.allocate(proxyBuffer.getBuffer().capacity());
		if(merge!=null){
		for (int i = proxyBuffer.readIndex; i < proxyBuffer.writeIndex; i++) {
			byteBuffer.put(proxyBuffer.getByte(i));
		}}
		merge.onNewRecords(session.getDatabase(), byteBuffer);
        proxyBuffer.readIndex = tmpReadIndex;

	}

	@Override
	void onRsFinish(MySQLSession session, boolean success, String msg) throws IOException {
		if (callBack != null) {
			if (success == false) {
				this.errPkg = new ErrorPacket();
				MySQLPackageInf curMQLPackgInf = session.curMSQLPackgInf;
				session.proxyBuffer.readIndex = curMQLPackgInf.startPos;
				this.errPkg.read(session.proxyBuffer);
				session.getSessionAttrMap().remove(SessionKeyEnum.SESSION_KEY_CONN_IDLE_FLAG.getKey());
				revertPreBuffer();
				callBack.finished(session, this, success, this.errPkg);
			} else {
				session.getSessionAttrMap().remove(SessionKeyEnum.SESSION_KEY_CONN_IDLE_FLAG.getKey());
				revertPreBuffer();
				callBack.finished(session, null, success, null);
			}
		}
		if(merge!=null){
			System.out.println("=>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>.hahahahah");
			merge.rrs.countDown(session,()->{
				System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
				merge.onEOF();
			});
		}
		logger.debug("session[{}] load result finish", session);
		//@todo check
		session.unbindMycatSession();
	}
}
