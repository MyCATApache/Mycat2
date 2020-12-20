package io.mycat.sqlrecorder;

import io.mycat.ScheduleUtil;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public enum SqlRecorderRuntime implements SimpleAnalyzer {
    INSTANCE;

    private final ConcurrentLinkedDeque<SqlRecord> context = new ConcurrentLinkedDeque<>();
    private long now;

    SqlRecorderRuntime() {
        ScheduledExecutorService timer = ScheduleUtil.getTimer();
        now = System.currentTimeMillis();
        timer.scheduleAtFixedRate(() -> now = System.currentTimeMillis(), 0, 1, TimeUnit.MILLISECONDS);
    }

    @Override
    public List<SqlRecord> getRecords() {
        LinkedList<SqlRecord> sqlRecords = new LinkedList<>(context);
        List<SqlRecord> subs = sqlRecords.stream().flatMap(i -> i.getPhySqlRecords().stream()).collect(Collectors.toList());
        sqlRecords.addAll(subs);
        return sqlRecords;
    }

    public static long ONE_SECOND = TimeUnit.SECONDS.toMillis(1);

    @Override
    public void addSqlRecord(SqlRecord record) {
        if (record != null) {
            boolean anyAllow = true;
            boolean condition = (record.getEndTime() - record.getStartTime()) > ONE_SECOND;
            if (anyAllow||condition) {
                if (context.size() > 5000) {
                    context.removeFirst();
                }
                context.addLast(record);
            }
        }
    }

    public long now() {
        return now;
    }


}