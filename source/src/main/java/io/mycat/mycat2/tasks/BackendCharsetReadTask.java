package io.mycat.mycat2.tasks;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.beans.MySQLPackageInf;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.mysql.packet.QueryPacket;
import io.mycat.proxy.ProxyBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


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
 *
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
    private static final Map<Integer, String> FIELD_NAME_MAP = new HashMap<Integer, String>();

    static {
        FIELD_NAME_MAP.put(0, "Collation");
        FIELD_NAME_MAP.put(1, "Charset");
        FIELD_NAME_MAP.put(2, "Id");
        FIELD_NAME_MAP.put(3, "Default");
        FIELD_NAME_MAP.put(4, "Compiled");
        FIELD_NAME_MAP.put(5, "SortLen");
    }

    public BackendCharsetReadTask(MySQLSession mySQLSession) {
        this.mySQLSession = mySQLSession;
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
        this.mySQLSession.writeToChannel();
    }

    @Override
    void onRsColCount(MySQLSession session) {
        ProxyBuffer proxyBuffer = session.proxyBuffer;
        MySQLPackageInf curMQLPackgInf = session.curMSQLPackgInf;
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
        MySQLPackageInf curMQLPackgInf = session.curMSQLPackgInf;
        int rowDataIndex = curMQLPackgInf.startPos + MySQLPacket.packetHeaderSize;
        //读取每行的各列数据
        for (int i = 0; i < fieldCount; i++) {
            int lenc = (int) proxyBuffer.getLenencInt(rowDataIndex);
            rowDataIndex += proxyBuffer.getLenencLength(lenc);
            String text = proxyBuffer.getFixString(rowDataIndex, lenc);
            rowDataIndex += lenc;
            loadToDataSource(session, i, text);
        }
    }

    private void loadToDataSource(MySQLSession session, int colNum, String text) {
        logger.debug("{}={}", FIELD_NAME_MAP.get(colNum), text);
    }

    @Override
    void onRsFinish(MySQLSession session) {
        //结果集完成
    }
}
