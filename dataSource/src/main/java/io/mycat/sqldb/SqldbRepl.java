package io.mycat.sqldb;

import io.mycat.calcite.prepare.MycatCalcitePrepare;
import io.mycat.calcite.prepare.MycatPlan;
import io.mycat.calcite.prepare.PrepareManager;

import java.util.Collections;

public enum SqldbRepl {
    INSTANCE;
    final PrepareManager prepareManager = new PrepareManager();

   public  MycatPlan query(String defaultSchmeaName, String sql) {
        MycatCalcitePrepare preare = prepareManager.preare(defaultSchmeaName, sql);
        MycatPlan plan = preare.plan(Collections.emptyList());
        return plan;
    }
}