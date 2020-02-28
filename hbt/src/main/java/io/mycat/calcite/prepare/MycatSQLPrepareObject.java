package io.mycat.calcite.prepare;

import lombok.Getter;

@Getter
public abstract class MycatSQLPrepareObject extends MycatPrepareObject {

    protected final String defaultSchemaName;
    private final String sql;//存储原SQL,在多语句情况下不拆分,禁止子类访问

    public MycatSQLPrepareObject(String defaultSchemaName, String sql) {
        this(null, defaultSchemaName, sql);
    }

    public MycatSQLPrepareObject(Long id, String defaultSchemaName, String sql) {
        super(id);
        this.defaultSchemaName = defaultSchemaName;
        this.sql = sql;
    }
}