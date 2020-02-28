package io.mycat.calcite.prepare;

import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.PrepareMycatRowMetaData;

public abstract class MycatHbtPrepareObject extends MycatPrepareObject {
    private final int paramCount;


    public MycatHbtPrepareObject(Long id, int paramCount) {
        super(id);
        this.paramCount = paramCount;
    }

    @Override
    public MycatRowMetaData prepareParams() {
        return new PrepareMycatRowMetaData(paramCount);
    }

}