package io.mycat.calcite.prepare;

import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.PrepareMycatRowMetaData;
import io.mycat.upondb.PrepareObject;

public abstract class MycatHbtPrepareObject extends PrepareObject {
    private final int paramCount;


    public MycatHbtPrepareObject(Long id, int paramCount) {
        super(id,false);
        this.paramCount = paramCount;
    }

    @Override
    public MycatRowMetaData prepareParams() {
        return new PrepareMycatRowMetaData(paramCount);
    }

}