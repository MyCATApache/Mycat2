package io.mycat.calcite.prepare;

import io.mycat.beans.mycat.MycatRowMetaData;

import java.util.List;

public final class MycatDelegateSQLPrepareObject extends MycatSQLPrepareObject {
    final MycatPrepareObject prepareObject;

    public MycatDelegateSQLPrepareObject(String defaultSchemaName, String sql, MycatPrepareObject prepareObject) {
        super(defaultSchemaName, sql);
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