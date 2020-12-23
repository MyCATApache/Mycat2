package io.mycat;

import io.mycat.calcite.table.AbstractMycatTable;

public interface MycatTableFactory {
    AbstractMycatTable create(String schemaName, String createTableSql, DrdsConst drdsConst);
}