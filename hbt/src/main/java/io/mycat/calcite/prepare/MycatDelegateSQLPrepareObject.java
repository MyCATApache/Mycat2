package io.mycat.calcite.prepare;

import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.upondb.UponDBContext;

import java.util.List;

public final class MycatDelegateSQLPrepareObject extends MycatSQLPrepareObject {
    final PrepareObject prepareObject;

    public MycatDelegateSQLPrepareObject(Long id,UponDBContext uponDBContext, String sql, PrepareObject prepareObject) {
        super(id,uponDBContext, sql);
        this.prepareObject = prepareObject;
    }

    @Override
    public MycatRowMetaData prepareParams() {
        return prepareObject.prepareParams();
    }

    @Override
    public MycatRowMetaData resultSetRowType() {
        return prepareObject.resultSetRowType();
    }

    @Override
    public PlanRunner plan(List<Object> params) {
        return prepareObject.plan(params);
    }

}