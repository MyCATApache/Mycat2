package io.mycat;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class TableCollector {

    public static Map<String, Collection<String>> collect(String defaultSchema, String sql) {
        Map<String, Collection<String>> collectionMap = new HashMap<>();
        try {
            SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
            sqlStatement.accept(new MySqlASTVisitorAdapter() {
                @Override
                public boolean visit(SQLExprTableSource x) {
                    String schema = x.getSchema();
                    String tableName = x.getTableName();
                    if (schema == null) {
                        schema = defaultSchema;
                    }
                    if (schema == null) {
                        throw new UnsupportedOperationException("please use schema");
                    }
                    schema = SQLUtils.normalize(schema);
                    tableName = SQLUtils.normalize(tableName);
                    Collection<String> strings = collectionMap.computeIfAbsent(schema, s -> new HashSet<>());
                    strings.add(tableName);
                    return super.visit(x);
                }
            });
        } catch (Throwable ignored) {

        }
        return collectionMap;
    }
}