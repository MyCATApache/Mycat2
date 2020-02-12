package io.mycat.calcite.prepare;

import io.mycat.calcite.MycatCalciteContext;
import io.mycat.calcite.MycatCalcitePlanner;
import lombok.SneakyThrows;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

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