package io.mycat.proxy.task;

import io.mycat.beans.mysql.MySQLCollationIndex;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.session.MySQLSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackendCharsetReadTask implements QueryResultSetTask {
    private final MySQLCollationIndex collationIndex;
    public int columnCount;

    protected final static Logger logger = LoggerFactory.getLogger(BackendCharsetReadTask.class);

    public BackendCharsetReadTask(MySQLCollationIndex collationIndex) {
        super();
        this.collationIndex = collationIndex;
    }

    public void request(MySQLSession sqlSession, AsynTaskCallBack<MySQLSession> callBack){
        request(sqlSession,"SHOW COLLATION;",callBack);
    }

    @Override
    public void onColumnDef(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }

    @Override
    public void onTextRow(MySQLPacket mySQLPacket, int startPos, int endPos) {
        int rowDataIndex = startPos + MySQLPacket.getPacketHeaderSize();

        String collation = null;
        String charset = null;
        int id = 0;

        //读取每行的各列数据
        for (int i = 0; i < columnCount; i++) {
            int lenc = (int) mySQLPacket.getLenencInt(rowDataIndex);
            rowDataIndex += MySQLPacket.getLenencLength(lenc);
            String text = mySQLPacket.getFixString(rowDataIndex, lenc);
            rowDataIndex += lenc;

            if (i == 0) {
                collation = text;
            } else if (i == 1) {
                charset = text;
            } else if (i == 2) {
                id = Integer.parseInt(text);
            } else {
                collationIndex.put(id, charset);
                break;
            }
        }
    }

    @Override
    public void onBinaryRow(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }

    @Override
    public void onColumnCount(int columnCount) {
        this.columnCount = columnCount;
    }
}
