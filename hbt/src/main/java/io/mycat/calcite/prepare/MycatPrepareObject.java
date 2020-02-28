package io.mycat.calcite.prepare;

import io.mycat.beans.mycat.MycatRowMetaData;
import lombok.Getter;

import java.util.List;

@Getter
public abstract class MycatPrepareObject {
    public MycatPrepareObject(Long id) {
        this.id = id;
    }

    final Long id;

    public abstract MycatRowMetaData prepareParams();

    public abstract MycatRowMetaData resultSetRowType();

    public abstract PlanRunner plan(List<Object> params);

}