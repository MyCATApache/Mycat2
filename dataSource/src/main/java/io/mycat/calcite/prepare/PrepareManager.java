/**
 * Copyright (C) <2019>  <chen junwen>
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
package io.mycat.calcite.prepare;

import io.mycat.calcite.MycatCalciteContext;
import io.mycat.calcite.MycatCalcitePlanner;
import lombok.SneakyThrows;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Junwen Chen
 **/
public class PrepareManager {
    private static final AtomicLong PREPARE_ID_GENERATOR = new AtomicLong(0);
    private ConcurrentHashMap<Long, MycatCalcitePrepare> PREPARE_MAP = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, MycatCalcitePrepare> SQL_PREPASRE = new ConcurrentHashMap<>();

    //sync with lock
    public MycatCalcitePrepare preare(String defaultSchemaName, String sql) {
        return SQL_PREPASRE
                .computeIfAbsent(sql, s ->
                        PREPARE_MAP.computeIfAbsent(PREPARE_ID_GENERATOR.incrementAndGet(),
                                id -> preare(defaultSchemaName, sql, id)));
    }

    public void close(Long id) {
        MycatCalcitePrepare mycatCalcitePrepare = PREPARE_MAP.get(id);
        if (mycatCalcitePrepare != null) {
            close(mycatCalcitePrepare);
        }
    }

    private synchronized void close(MycatCalcitePrepare prepare) {
        SQL_PREPASRE.remove(prepare.getSql());
        PREPARE_MAP.remove(prepare.getId());
    }


    @SneakyThrows
    public MycatCalcitePrepare preare(String defaultSchemaName, String sql, Long id) {
        MycatCalcitePlanner planner = MycatCalciteContext.INSTANCE.createPlanner(defaultSchemaName);
        SqlNode sqlNode = planner.parse(sql);
        SqlValidatorImpl sqlValidator = planner.getSqlValidator();
        RelDataType parameterRowType = sqlValidator.getParameterRowType(sqlNode);
        return new MycatCalcitePrepare(id, defaultSchemaName, sql, sqlNode, parameterRowType);
    }
}