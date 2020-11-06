package io.mycat.calcite.prepare;

import io.mycat.PlanRunner;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.upondb.MycatDBContext;
import io.mycat.upondb.PrepareObject;
import lombok.Getter;

import java.util.List;

@Getter
public final class MycatDelegateSQLPrepareObject extends MycatSQLPrepareObject {
    final PrepareObject prepareObject;

    public MycatDelegateSQLPrepareObject(Long id, MycatDBContext uponDBContext, String sql, PrepareObject prepareObject) {
        super(id,uponDBContext, sql,prepareObject.isForUpdate());
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