/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.sqlrecorder;

import io.mycat.ScheduleUtil;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public enum SqlRecorderRuntime implements SimpleAnalyzer {
    INSTANCE;

    public static long ONE_SECOND = TimeUnit.SECONDS.toMillis(1);
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

    @Override
    public void addSqlRecord(SqlRecord record) {
        if (record != null) {
            boolean anyAllow = true;
            boolean condition = (record.getEndTime() - record.getStartTime()) > ONE_SECOND;
            if (anyAllow || condition) {
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