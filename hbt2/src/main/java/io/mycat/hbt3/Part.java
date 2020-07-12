package io.mycat.hbt3;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.util.SqlString;

public interface Part {
    int getMysqlIndex();

    int getSchemaIndex();

    SqlString getSql(RelNode node);
    public String getBackendTableName(MycatTable mycatTable);

    public String getBackendSchemaName(MycatTable mycatTable);

}