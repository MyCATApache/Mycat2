package io.mycat.hbt3;

public interface MycatTableFactory {
    AbstractMycatTable create(String schemaName, String createTableSql, DrdsConst drdsConst);
}