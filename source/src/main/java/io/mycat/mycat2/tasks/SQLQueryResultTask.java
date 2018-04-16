package io.mycat.mycat2.tasks;

import io.mycat.mycat2.ColumnMeta;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.console.SessionKeyEnum;
import io.mycat.mycat2.hbt.TableMeta;
import io.mycat.mycat2.net.DefaultMycatSessionHandler;
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

public class SQLQueryResultTask extends RawSQLQueryResultTaskWrapper {
    private static Logger logger = LoggerFactory.getLogger(SQLQueryResultTask.class);
    AbstractDataNodeMerge merge;
    int fieldCount = 0;
    int getFieldCount = 0;
    Map<String, ColumnMeta> columToIndx = new HashMap<>();

    public SQLQueryResultTask(AbstractDataNodeMerge merge) {
        this.merge = merge;
    }

    @Override
    void onRsColCount(MySQLSession session, int fieldCount) {
        this.fieldCount = fieldCount;
    }

    @Override
    void onRsColDef(MySQLSession session, String catalog, String schema, String table, String orgTable, String name, String originName, byte filler, int charsetNumber, int length, int fieldType) {
        columToIndx.put(name, new ColumnMeta(getFieldCount++, fieldType));
        if (fieldCount == getFieldCount) {
            merge.onRowMetaData(columToIndx, fieldCount);
        }
    }

    @Override
    void onRsRow(MySQLSession session, ProxyBuffer proxyBuffer) {
//        ArrayList<byte[]> row = new ArrayList<byte[]>(3);
//        for(int i = 0; i < 3; i++) {
//            byte[] x = proxyBuffer.readLenencBytes();
//            ByteBuffer byteBuffer =ByteBuffer.allocate(proxyBuffer.getBuffer().position());;
//            byteBuffer.put(x);
//            //byteBuffer.flip();
//            merge.onNewRecords("",byteBuffer);
//            row.add(x);
//        }

        ByteBuffer byteBuffer = ByteBuffer.allocate(proxyBuffer.getBuffer().capacity());
        for (int i = proxyBuffer.readIndex; i < proxyBuffer.writeIndex; i++) {
            byteBuffer.put(proxyBuffer.getByte(i));
        }
        merge.onNewRecords("", byteBuffer);



//        for(int i = 0; i < 3; i++) {
//            System.out.println(row);
//            byteBuffer.put(row.get(i));
//
//        }

}


    @Override
    void onRsFinish(MySQLSession session, boolean success, String msg) throws IOException {
        merge.onEOF();
        System.out.println(msg);
        session.setCurNIOHandler(DefaultMycatSessionHandler.INSTANCE);
    }
}
