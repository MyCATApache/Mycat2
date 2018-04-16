package io.mycat.mycat2.tasks;

import io.mycat.mycat2.ColumnMeta;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.PackWraper;
import io.mycat.mycat2.console.SessionKeyEnum;
import io.mycat.mycat2.hbt.TableMeta;
import io.mycat.mycat2.route.RouteResultset;
import io.mycat.proxy.ProxyBuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class HeapDataNodeMergeManager extends AbstractDataNodeMerge {
    TableMeta tableMeta;
    /**
     * 标志业务线程是否启动了？
     */
    protected final AtomicBoolean running = new AtomicBoolean(false);

    public HeapDataNodeMergeManager(RouteResultset rrs, MycatSession mycatSession) {
        super(rrs, mycatSession);
    }

    @Override
    public void onRowMetaData(Map<String, ColumnMeta> columToIndx, int fieldCount) {
        if (tableMeta == null) {
            tableMeta = new TableMeta();
            tableMeta.init(fieldCount);
            Set<Map.Entry<String, ColumnMeta>> entries = columToIndx.entrySet();
            for (Map.Entry<String, ColumnMeta> entry : entries) {
                tableMeta.headerResultSetMeta.addFiled(entry.getKey(), entry.getValue().colType);
            }
        }

    }

    @Override
    public Iterator<ByteBuffer> getResults(byte[] eof) {
        return null;
    }


    @Override
    public void clear() {
        this.tableMeta = null;
    }

    @Override
    public void run() {
        // sort-or-group: no need for us to using multi-threads, because
        //both sorter and group are synchronized!!
        // @author Uncle-pan
        // @since 2016-03-23
        if (!running.compareAndSet(false, true)) {
            return;
        }

        // eof handler has been placed to "if (pack == END_FLAG_PACK){}" in for-statement
        // @author Uncle-pan
        // @since 2016-03-23
        boolean nulpack = false;
        try {
            // loop-on-packs
            for (; ; ) {
                final PackWraper pack = packs.take();
                System.out.println(packs.size());
                // async: handling row pack queue, this business thread should exit when no pack
                // @author Uncle-pan
                // @since 2016-03-23
                if (pack == null) {
                    nulpack = true;
                    break;
                }
                if (pack == END_FLAG_PACK) {
                    System.out.println("END_FLAG_PACK");
                    ProxyBuffer proxyBuffer = mycatSession.proxyBuffer;
                    proxyBuffer.reset();
                    tableMeta.writeBegin(proxyBuffer);
                    tableMeta.writeRowData(proxyBuffer);
                    proxyBuffer.flip();
                    proxyBuffer.readIndex = proxyBuffer.writeIndex;
                    mycatSession.takeBufferOwnerOnly();
                    if (!tableMeta.isWriteFinish()) {
                        mycatSession.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_HBT_TABLE_META.getKey(), tableMeta);
                    }
                    try {
                        System.out.println("开始发送");
                        mycatSession.writeToChannel();
                        clear();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
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
        } catch (final Exception e) {
            e.printStackTrace();
            // multiQueryHandler.handleDataProcessException(e);
        } finally {
            running.set(false);
        }
        // try to check packs, it's possible that adding a pack after polling a null pack
        //and before this time pointer!!
        // @author Uncle-pan
        // @since 2016-03-23
        if (nulpack && !packs.isEmpty()) {
            this.run();
        }
    }
}
