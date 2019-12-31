package io.mycat.sqlEngine.ast.extractor;

import com.alibaba.fastsql.DbType;
import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.fastsql.sql.repository.SchemaObject;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class MysqlTableExtractor extends MySqlASTVisitorAdapter {
    private Set<SchemaTablePair> dbSet = null;
    private String schemaName;

    public MysqlTableExtractor(String schemaName) {
        this.schemaName = schemaName;
    }

    @Override
    public boolean visit(SQLExprTableSource x) {
        SchemaObject schemaObject = x.getSchemaObject();
        if (schemaObject != null) {
            addDbTable(schemaObject.getSchema().getName(), schemaObject.getSchema().getName());
        } else {
            String schemaName = x.getSchema();
            String tableName = x.getTableName();
            if (schemaName != null) {
                schemaName = SQLUtils.normalize(schemaName, DbType.mysql);
            }
            if (tableName != null) {
                tableName = SQLUtils.normalize(tableName, DbType.mysql);
            }
            if (schemaName == null){
                schemaName = this.schemaName;
            }
            addDbTable(schemaName, tableName);
        }
        return super.visit(x);
    }


    public void addDbTable(String schemaName, String tableName) {
        if (dbSet == null) {
            dbSet = new HashSet<>();
        }
        dbSet.add(new SchemaTablePair(schemaName, tableName));
    }

    public Set<SchemaTablePair> getDbSet() {
        return dbSet == null ? Collections.emptySet() : dbSet;
    }

}