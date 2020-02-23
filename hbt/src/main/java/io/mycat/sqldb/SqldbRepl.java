package io.mycat.sqldb;

import io.mycat.calcite.prepare.MycatCalcitePrepare;
import io.mycat.calcite.prepare.MycatPlan;
import io.mycat.calcite.prepare.PrepareManager;

import java.util.Collections;

public enum SqldbRepl {
    INSTANCE;
    final PrepareManager prepareManager = new PrepareManager();

    public MycatPlan querySQL(String defaultSchmeaName, String sql) {
        MycatCalcitePrepare preare = prepareManager.preare(defaultSchmeaName, sql);
        return preare.plan(Collections.emptyList());
    }

}