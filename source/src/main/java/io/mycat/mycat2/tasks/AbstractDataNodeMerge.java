package io.mycat.mycat2.tasks;

import io.mycat.mycat2.ColumnMeta;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.PackWraper;
import io.mycat.mycat2.route.RouteResultset;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class AbstractDataNodeMerge implements Runnable {

    private static Logger LOGGER = Logger.getLogger(AbstractDataNodeMerge.class);
    /**
     * row 有多少col
     */
    protected int fieldCount;

    /**
     * 本次select的路由缓存集
     */
    protected final RouteResultset rrs;
    /**
     * 夸分片处理handler
     */
    protected MultiNodeQueryHandler multiQueryHandler = null;

    /**
     * 是否执行流式结果集输出
     */

    protected boolean isStreamOutputResult = false;

    /**
     * rowData缓存队列
     */
    protected LinkedBlockingQueue<PackWraper> packs = new LinkedBlockingQueue<>();

    MycatSession mycatSession;
    /**
     * 分片结束包
     */
    protected static final PackWraper END_FLAG_PACK = new PackWraper(null,null);
    public AbstractDataNodeMerge(RouteResultset rrs, MycatSession mycatSession) {
        this.rrs = rrs;
        this.mycatSession = mycatSession;;
        this.executor = Executors.newSingleThreadExecutor();
    }
    public void onEOF() {
        packs.add(END_FLAG_PACK);
        executor.submit(this);
    }
    ExecutorService executor;

    public boolean onNewRecords(String repName, ByteBuffer rowData) {
        /*
        读取的数据范围是 readIndex --- writeIndex 之间的数据.
         */
        System.out.println("onNewRecords"+repName + rowData);
        if(packs.offer(new PackWraper(rowData,repName))){
            executor.submit(this);
            return true;
        }else{
            return false;
        }
    }

    public abstract void onRowMetaData(Map<String, ColumnMeta> columToIndx, int fieldCount);

    public RouteResultset getRrs() {
        return this.rrs;
    }

    /**
     * 做最后的结果集输出
     * @return (最多i*(offset+size)行数据)
     */
    public abstract Iterator<ByteBuffer> getResults(byte[] eof);
    public abstract void clear();
}
