package io.mycat.calcite.shardingQuery;

import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import io.mycat.calcite.BackEndTableInfo;

import java.util.Set;

public class Rrs {
    Set<BackEndTableInfo> backEndTableInfos;
    SQLExprTableSource table;

    public Rrs(Set<BackEndTableInfo> backEndTableInfos, SQLExprTableSource table) {
        this.backEndTableInfos = backEndTableInfos;
        this.table = table;
    }

    public Set<BackEndTableInfo> getBackEndTableInfos() {
        return backEndTableInfos;
    }

    public SQLExprTableSource getTable() {
        return table;
    }
}
