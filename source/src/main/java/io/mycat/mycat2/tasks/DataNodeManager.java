package io.mycat.mycat2.tasks;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.PackWraper;
import io.mycat.mycat2.beans.ColumnMeta;
import io.mycat.mycat2.route.RouteResultset;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.mysql.packet.RowDataPacket;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class DataNodeManager implements Runnable {

    /**
     * 分片结束包
     */
    protected static final PackWraper END_FLAG_PACK = new PackWraper(null, null);
    private static Logger LOGGER = Logger.getLogger(DataNodeManager.class);

    /**
    * 标志业务线程是否启动了？
    */
    protected final AtomicBoolean running = new AtomicBoolean(false);
    
    /**
     * row 有多少col
     */
    protected int fieldCount;
    /**
     * 本次select的路由缓存集
     */
    protected RouteResultset routeResultset;
    
    /**
     * 将Map对应的col字段集，返回row中对应的index数组
     * @param columns
     * @param toIndexMap
     * @return
     */
    protected static int[] toColumnIndex(String[] columns, Map<String, ColumnMeta> toIndexMap) {
        int[] result = new int[columns.length];
        ColumnMeta curColMeta;
        for (int i = 0; i < columns.length; i++) {
            curColMeta = toIndexMap.get(columns[i]);
            if (curColMeta == null) {
                throw new IllegalArgumentException(
                        "all columns in group by clause should be in the selected column list.!"
                                + columns[i]);
            }
            result[i] = curColMeta.colIndex;
        }
        return result;
    }
    
    /**
     * rowData缓存队列
     */
    protected LinkedBlockingQueue<PackWraper> packs = new LinkedBlockingQueue<>();

    MycatSession mycatSession;
    ExecutorService executor;
    ArrayList<SQLQueryStream> backendStreams = new ArrayList<>();


    public DataNodeManager(RouteResultset routeResultset, MycatSession mycatSession) {
        this.routeResultset = routeResultset;
        this.mycatSession = mycatSession;
        this.executor = Executors.newSingleThreadExecutor();
    }

    protected void init(RouteResultset routeResultset, MycatSession mycatSession) {
        this.routeResultset = routeResultset;
        this.mycatSession = mycatSession;
        this.executor = Executors.newSingleThreadExecutor();
    }


    public void onEOF(String dataNode) {
        packs.add(END_FLAG_PACK);
        executor.submit(this);
    }

    public abstract void onError(String dataNode, String msg);

    public boolean onNewRecords(String dataNode, ByteBuffer rowData) {
        /*
        	读取的数据范围是 readIndex --- writeIndex 之间的数据.
         */
        System.out.println("onNewRecords" + dataNode + rowData);
        if (packs.offer(new PackWraper(rowData, dataNode))) {
            executor.submit(this);
            return true;
        } else {
            return false;
        }
    }

    public abstract void onRowMetaData(String dataNode, Map<String, ColumnMeta> columToIndx, int fieldCount);

    public RouteResultset getRouteResultset() {
        return this.routeResultset;
    }

    /**
     * 做最后的结果集输出
     *
     * @return (最多i * ( offset + size)行数据)
     */
    public abstract List<RowDataPacket> getResults();

    public void clearResouces() {
        this.packs.clear();
        this.fieldCount = 0;
        this.routeResultset = null;
        clearSQLQueryStreamResouces();
    }

    public void close(boolean normal, String error) {
        this.executor.shutdown();
        unbindSQLQueryStreams(normal, error);
        this.packs = null;
        this.executor = null;
        // mycatSession.merge = null;
    }

    public abstract void onfinished();


    public void closeMutilBackendAndResponseError(boolean normal, int errno, String error) {
        clearResouces();
        unbindSQLQueryStreams(normal, error);
        close(normal, error);
        this.mycatSession.takeBufferOwnerOnly();
        try {
            this.mycatSession.sendErrorMsg(errno, error);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void closeMutilBackendAndResponseError(boolean normal, ErrorPacket error) {
        clearResouces();
        unbindSQLQueryStreams(normal, error.message);
        close(normal, error.message);
        this.mycatSession.takeBufferOwnerOnly();
        try {
            this.mycatSession.responseOKOrError(error);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 此方法不会进行 bind 操作
     *
     * @param
     */
    public void addSQLQueryStream(SQLQueryStream stream) {
        this.backendStreams.add(stream);
    }

    public void unbindSQLQueryStreams(boolean normal, String error) {
        Iterator<SQLQueryStream> iterator = this.backendStreams.iterator();
        while (iterator.hasNext()) {
            MySQLSession mysqlsession = iterator.next().session;
            this.mycatSession.unbindBeckend(mysqlsession);
            if (!normal) {
                mysqlsession.close(normal, error);
            }
            iterator.remove();// ArrayList iterator support remove to without advantage loop
        }
    }

    /**
     * 正常情况下unbind
     * 解除 backend mysql  session 对 mycat session 的绑定 不解除 mycat 对 mysql  session 的缓存
     * 即不清除 backendMap 对 mysql session的引用
     * cjw
     */
    public void clearSQLQueryStreamResouces() {
        for (SQLQueryStream backend : this.backendStreams) {
            backend.clearResouces();
        }
    }

    public boolean isMultiBackendMoreOne() {
        return backendStreams.size() > 1;
    }
}
