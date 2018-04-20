package io.mycat.mycat2.tasks;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.beans.ColumnMeta;
import io.mycat.mysql.packet.QueryPacket;
import io.mycat.proxy.ProxyBuffer;

/**
 * @author <a href="mailto:karakapi@outlook.com">jamie</a>
 * @since 2.0
 */
public class SQLQueryStream extends RawSQLQueryResultTaskWrapper {
    private static Logger logger = LoggerFactory.getLogger(SQLQueryStream.class);
    final String dataNode;
    final private DataNodeManager merge;
    final private Map<String, ColumnMeta> columToIndx = new LinkedHashMap<>();
    int fieldCount = 0;
    int getFieldCount = 0;

    public SQLQueryStream(String dataNode, MySQLSession optSession, DataNodeManager merge) {
        this.useNewBuffer = true;
        setSession(optSession, true, false);
        this.session = optSession;
        this.merge = merge;
        this.dataNode = dataNode;
    }


    public void fetchSQL(String sql) throws IOException {
        QueryPacket queryPacket = new QueryPacket();
        queryPacket.packetId = 0;
        queryPacket.sql = sql;
        fetchSQL(queryPacket);
    }

    @Override
    void onRsColCount(MySQLSession session, int fieldCount) {
        this.fieldCount = fieldCount;
    }

    @Override
    void onRsColDef(MySQLSession session, Object catalog, Object schema, Object table, Object orgTable, Object name, Object org_name, byte filler, int charsetNumber, int length, int fieldType) {
        columToIndx.put((String) name, new ColumnMeta(getFieldCount++, fieldType));
        if (fieldCount == getFieldCount) {
            merge.onRowMetaData(this.dataNode, columToIndx, fieldCount);
        }
    }

    @Override
    void onRsRow(MySQLSession session, ProxyBuffer proxyBuffer) {
        ByteBuffer byteBuffer = session.bufPool.allocate(proxyBuffer.writeIndex - proxyBuffer.readIndex);
        ByteBuffer data = proxyBuffer.getBuffer();
        data.position(proxyBuffer.readIndex);
        data.limit(proxyBuffer.writeIndex);
        byteBuffer.put(data);//copy with internal methods
        merge.onNewRecords(this.dataNode, byteBuffer);
    }

    @Override
    void onError(MySQLSession session, String msg) {
        if (logger.isDebugEnabled()) {
            logger.debug("mysql session:{} is error", session.toString());
        }
        merge.onError(this.dataNode, msg);
    }

    @Override
    void onRsFinished(MySQLSession session) {
        merge.routeResultset.countDown(session, () -> {
            if (logger.isDebugEnabled()) {
                logger.debug("mysql session:{} is last  finished", session.toString());
            }
            merge.onEOF(this.dataNode);
        });
    }

    public DataNodeManager getMerge() {
        return merge;
    }
}
