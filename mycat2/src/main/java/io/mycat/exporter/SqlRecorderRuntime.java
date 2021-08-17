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
package io.mycat.exporter;

import io.mycat.monitor.SqlEntry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;

public enum SqlRecorderRuntime implements SimpleAnalyzer {
    INSTANCE;

    public static long ONE_SECOND = TimeUnit.SECONDS.toMillis(1);
    private final ConcurrentLinkedDeque<SqlEntry> context = new ConcurrentLinkedDeque<>();


    SqlRecorderRuntime() {

    }

    @Override
    public List<SqlEntry> getRecords() {
        ArrayList<SqlEntry> sqlEntries = new ArrayList<>(context);
        Collections.sort(sqlEntries);
        return sqlEntries;
    }

    @Override
    public void addSqlRecord(SqlEntry record) {
        if (record != null) {
            boolean condition = record.getSqlTime() > ONE_SECOND;
            if (condition) {
                if (context.size() > 5000) {
                    ArrayList<SqlEntry> sqlEntries = new ArrayList<>(context);
                    Collections.sort(sqlEntries);
                    context.clear();
                    context.removeLast();
                }
                context.addLast(record);
            }
        }
    }
}