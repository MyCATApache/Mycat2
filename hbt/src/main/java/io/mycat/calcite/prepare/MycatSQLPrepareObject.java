package io.mycat.calcite.prepare;

import io.mycat.upondb.UponDBContext;
import lombok.Getter;

@Getter
public abstract class MycatSQLPrepareObject extends PrepareObject {

    protected final UponDBContext uponDBContext;
    protected final String sql;//存储原SQL,在多语句情况下不拆分,禁止子类访问



    public MycatSQLPrepareObject(Long id, UponDBContext uponDBContext, String sql) {
        super(id);
        this.uponDBContext = uponDBContext;
        this.sql = sql;
    }
}