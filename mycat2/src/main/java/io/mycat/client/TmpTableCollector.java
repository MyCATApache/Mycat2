package io.mycat.client;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import io.mycat.metadata.MetadataManager;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

public class TmpTableCollector {
    final String sql;
    final HashMap<String, Collection<String>> map = new HashMap<>();
    public TmpTableCollector(String defaultSchema,String sql) {
        this.sql = sql;

        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
        MetadataManager.INSTANCE.resolveMetadata(sqlStatement);
        sqlStatement.accept(new MySqlASTVisitorAdapter(){
            @Override
            public void endVisit(SQLExprTableSource x) {
                String schema = x.getSchema() == null ? defaultSchema : x.getSchema();
                String table = SQLUtils.normalize(x.getTableName());
                Collection<String> strings = map.computeIfAbsent(schema.toLowerCase(), s -> new HashSet<>());
                strings.add(table);
                super.endVisit(x);
            }
        });
    }

    public HashMap<String, Collection<String>> getMap() {
        return map;
    }

    public Collection<String> getSchemas(){
        return map.keySet();
    }
}