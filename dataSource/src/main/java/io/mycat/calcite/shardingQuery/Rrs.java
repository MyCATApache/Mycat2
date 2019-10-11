package io.mycat.calcite.shardingQuery;

import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import io.mycat.calcite.BackendTableInfo;

import java.util.Set;

public class Rrs {
    Set<BackendTableInfo> backEndTableInfos;
    SQLExprTableSource table;

    public Rrs(Set<BackendTableInfo> backEndTableInfos, SQLExprTableSource table) {
        this.backEndTableInfos = backEndTableInfos;
        this.table = table;
    }

    public Set<BackendTableInfo> getBackEndTableInfos() {
        return backEndTableInfos;
    }

    public SQLExprTableSource getTable() {
        return table;
    }
}
