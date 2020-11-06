package io.mycat.calcite.prepare;

import io.mycat.upondb.MycatDBContext;
import io.mycat.upondb.PrepareObject;
import lombok.Getter;

@Getter
public abstract class MycatSQLPrepareObject extends PrepareObject {

    protected final MycatDBContext uponDBContext;
    protected final String sql;//存储原SQL,在多语句情况下不拆分,禁止子类访问



    public MycatSQLPrepareObject(Long id, MycatDBContext uponDBContext, String sql,boolean forUpdate) {
        super(id,forUpdate);
        this.uponDBContext = uponDBContext;
        this.sql = sql;
    }
}