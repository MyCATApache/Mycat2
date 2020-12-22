package io.mycat.sqlrecorder;

import lombok.Data;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Data
public class SqlRecord implements Comparable<SqlRecord> {
    private static final AtomicLong IDS = new AtomicLong();
    public List<SqlRecord> phySqlRecords = new ArrayList<>();
    private long id;
    private Object sql;            //SQL
    private long sqlRows;        //结果集行数或者影响的行数
    private long startTime;        //开始时间
    //    private long parseStartTime; //解析sql开始时间
//    private long parseEndTime; //解析sql结束时间
//    private long compileSqlStartTime; //编译sql开始时间
//    private long compileSqlEndTime; //编译sql结束时间
//    private long rboStartTime; //RBO开始时间
//    private long rboEndTime; //RBO结束时间
//    private long connectionStartTime; //获得连接开始时间
//    private long connectionEndTime; //获得连接结束时间
    private long endTime;        //结束时间
    private String target;

    public SqlRecord() {
        id = IDS.getAndIncrement();
    }

    public SqlRecord(long id) {
        this.id = id;
    }

    public static long now() {
        return SqlRecorderRuntime.INSTANCE.now();
    }
//
//    public void setConnectionEndTime() {
//        connectionEndTime = SqlRecorderRuntime.INSTANCE.now();
//    }
//
//    public void setStartTime() {
//        connectionEndTime = SqlRecorderRuntime.INSTANCE.now();
//    }
//
//    public void setConnectionStartTime() {
//        connectionStartTime = SqlRecorderRuntime.INSTANCE.now();
//    }

    public static SqlRecord create() {
        return new SqlRecord();
    }

    public void setEndTime() {
        endTime = SqlRecorderRuntime.INSTANCE.now();
    }

    public void addSubRecord(Object sql,
                             long startTime,
                             String host,
                             long rowCount) {
        addSubRecord(sql, startTime, SqlRecord.now(), host, rowCount);
    }

    public void addSubRecord(Object sql,
                             long startTime,
                             long endTime,
                             String host,
                             long rowCount) {
        SqlRecord sqlRecord = new SqlRecord(this.id);
        sqlRecord.setSql(sql);
        sqlRecord.setStartTime(startTime);
        sqlRecord.setEndTime(endTime);
        sqlRecord.setTarget(host);
//        sqlRecord.setConnectionStartTime(connectionStartTime);
//        sqlRecord.setConnectionEndTime(connectionEndTime);
        sqlRecord.setSqlRows(rowCount);
        phySqlRecords.add(sqlRecord);
    }

    @Override
    public int compareTo(@NotNull SqlRecord o) {
        long l = this.getEndTime() - this.getStartTime();
        long r = o.getEndTime() - this.getStartTime();
        return (int) (l - r);
    }

    public long getExecuteTime() {
        return endTime - startTime;
    }
}
