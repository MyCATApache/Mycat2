package io.mycat.upondb;

import io.mycat.beans.mycat.MycatRowMetaData;
import lombok.Getter;

import java.util.List;

@Getter
public abstract class PrepareObject {
    public PrepareObject(Long id,boolean forUpdate) {
        this.id = id;
        this.forUpdate = forUpdate;
    }

    final Long id;
    final boolean forUpdate;

    public abstract MycatRowMetaData prepareParams();

    public abstract MycatRowMetaData resultSetRowType();

    public abstract PlanRunner plan(List<Object> params);

}