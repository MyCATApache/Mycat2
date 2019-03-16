package io.mycat.mycat2.tasks;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.beans.MySQLMetaBean;
import io.mycat.mysql.MySQLPacketInf;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.mysql.packet.QueryPacket;
import io.mycat.proxy.ProxyBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 * Created by ynfeng on 2017/8/28.
 * <p>
 * 读取msyql字符集映射
 * <p>
 * <pre>
 * 字符集对应关系查询:
 *
 * SHOW COLLATION
 * +--------------------------+----------+-----+---------+----------+---------+
 * | Collation                | Charset  | Id  | Default | Compiled | Sortlen |
 * +--------------------------+----------+-----+---------+----------+---------+
 * | big5_chinese_ci          | big5     |   1 | Yes     | Yes      |       1 |
 * | big5_bin                 | big5     |  84 |         | Yes      |       1 |
 * | dec8_swedish_ci          | dec8     |   3 | Yes     | Yes      |       1 |
 * | dec8_bin                 | dec8     |  69 |         | Yes      |       1 |
 * </pre>
 * <p>
 * 简单使用示例
 * <pre>
 *    BackendCharsetReadTask backendCharsetReadTask = new BackendCharsetReadTask(optSession);
 *    optSession.setCurNIOHandler(backendCharsetReadTask);
 *    backendCharsetReadTask.readCharset();
 * </pre>
 */
public class BackendCharsetReadTask extends BackendIOTaskWithResultSet<MySQLSession> {
    private static Logger logger = LoggerFactory.getLogger(BackendCharsetReadTask.class);
    private static final String SQL = "SHOW COLLATION;";
    private MySQLSession mySQLSession;
    private int fieldCount;
    private MySQLMetaBean mySQLMetaBean;

    public BackendCharsetReadTask(MySQLSession mySQLSession, MySQLMetaBean mySQLMetaBean,AsynTaskCallBack<MySQLSession> callBack) {
        this.mySQLSession = mySQLSession;
        this.mySQLMetaBean = mySQLMetaBean;
        this.callBack = callBack;
    }

    public void readCharset() throws IOException {
        ProxyBuffer proxyBuf = mySQLSession.proxyBuffer;
        proxyBuf.reset();
        QueryPacket queryPacket = new QueryPacket();
        queryPacket.packetId = 0;
        queryPacket.sql = SQL;
        queryPacket.write(proxyBuf);
        proxyBuf.flip();
        proxyBuf.readIndex = proxyBuf.writeIndex;
        try {
        	this.mySQLSession.writeToChannel();
		} catch (IOException e) {
			onRsFinish(this.mySQLSession,false,e.getMessage());
		}
    }

    @Override
    void onRsColCount(MySQLSession session) {
        ProxyBuffer proxyBuffer = session.proxyBuffer;
        MySQLPacketInf curMQLPackgInf = session.curPacketInf;
        //读取有多少列
        fieldCount = (int) proxyBuffer.getLenencInt(curMQLPackgInf.startPos + MySQLPacket.packetHeaderSize);
    }

    @Override
    void onRsColDef(MySQLSession session) {
        //并不关心列定义
    }

    @Override
    void onRsRow(MySQLSession session) {
        ProxyBuffer proxyBuffer = session.proxyBuffer;
        MySQLPacketInf curMQLPackgInf = session.curPacketInf;
        int rowDataIndex = curMQLPackgInf.startPos + MySQLPacket.packetHeaderSize;

        String collation = null;
        String charset = null;
        int id = 0;

        //读取每行的各列数据
        for (int i = 0; i < fieldCount; i++) {
            int lenc = (int) proxyBuffer.getLenencInt(rowDataIndex);
            rowDataIndex += proxyBuffer.getLenencLength(lenc);
            String text = proxyBuffer.getFixString(rowDataIndex, lenc);
            rowDataIndex += lenc;

            if (i == 0) {
                collation = text;
            } else if (i == 1) {
                charset = text;
            } else if (i == 2) {
                id = Integer.parseInt(text);
            } else {
                mySQLMetaBean.INDEX_TO_CHARSET.put(id, charset);
                Integer index = mySQLMetaBean.CHARSET_TO_INDEX.get(charset);
                if (index == null || index > id) {
                    mySQLMetaBean.CHARSET_TO_INDEX.put(charset, id);
                }

                break;
            }
        }
    }

    @Override
    void onRsFinish(MySQLSession session,boolean success,String msg) throws IOException {
    	if(success){
    		if(callBack!=null){
        		callBack.finished(session, null, success, null);
        	}
            //结果集完成
            logger.debug("session[{}] load charset finish",session);
    	}else{
    		if(ResultSetState.RS_STATUS_READ_ERROR == curRSState||
    				ResultSetState.RS_STATUS_WRITE_ERROR == curRSState){
    			session.close(false, msg);
    		}
    	}
    }

}
