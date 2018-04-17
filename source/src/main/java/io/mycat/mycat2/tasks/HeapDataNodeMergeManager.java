package io.mycat.mycat2.tasks;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.PackWraper;
import io.mycat.mycat2.beans.ColumnMeta;
import io.mycat.mycat2.console.SessionKeyEnum;
import io.mycat.mycat2.hbt.TableMeta;
import io.mycat.mycat2.route.RouteResultset;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.util.ErrorCode;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class HeapDataNodeMergeManager extends DataNodeManager {
    private static Logger LOGGER = Logger.getLogger(HeapDataNodeMergeManager.class);
    TableMeta tableMeta;

    public HeapDataNodeMergeManager(RouteResultset rrs, MycatSession mycatSession) {
        super(rrs, mycatSession);
    }

    @Override
    public void onRowMetaData(String datanode, Map<String, ColumnMeta> columToIndx, int fieldCount) {
        if (tableMeta == null) {
            tableMeta = new TableMeta();
            tableMeta.init(fieldCount);
            Set<Map.Entry<String, ColumnMeta>> entries = columToIndx.entrySet();
            for (Map.Entry<String, ColumnMeta> entry : entries) {
                tableMeta.headerResultSetMeta.addField(entry.getKey(), entry.getValue().colType);
            }
        }

    }

    @Override
    public Iterator<ByteBuffer> getResults() {
        return null;
    }

    public TableMeta getTableMeta() {
        return tableMeta;
    }


    public void clear() {
        this.tableMeta = null;
        this.mycatSession.merge = null;
    }

    @Override
    public void run() {
        try {
            // loop-on-packs
            for (; ; ) {
                final PackWraper pack = packs.take();
                if (pack == END_FLAG_PACK) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("END_FLAG_PACK");
                    }
                    ProxyBuffer proxyBuffer = mycatSession.proxyBuffer;
                    proxyBuffer.reset();
                    tableMeta.writeBegin(proxyBuffer);
                    tableMeta.writeRowData(proxyBuffer);
                    proxyBuffer.flip();
                    proxyBuffer.readIndex = proxyBuffer.writeIndex;
                    mycatSession.takeBufferOwnerOnly();
                    if (!tableMeta.isWriteFinish()) {
                        mycatSession.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_MERGE_OVER_FLAG.getKey(), null);
                    }
                    mycatSession.writeToChannel();
                    return;
                } else {
                    ArrayList<byte[]> v = new ArrayList<>(tableMeta.fieldCount);
                    ProxyBuffer proxyBuffer = new ProxyBuffer(pack.rowData);
                    for (int i = 0; i < tableMeta.fieldCount; i++) {
                        byte[] value = proxyBuffer.readLenencBytes();
                        v.add(value);
                    }
                    tableMeta.addFieldValues(v);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            this.closeMutilBackendAndResponseError(false, ErrorCode.ER_UNKNOWN_ERROR, e.getMessage());
        }
    }

    @Override
    public void onError(String dataNode, String msg) {
        this.closeMutilBackendAndResponseError(false, ErrorCode.ER_UNKNOWN_ERROR, msg);
    }

    @Override
    public void onfinished() {
        clear();
        clearSQLQueryStreamResouces();
    }
}
