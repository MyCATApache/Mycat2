package cn.lightfish.sqlEngine.ast.extractor;

import cn.lightfish.sqlEngine.schema.MycatSchemaManager;
import cn.lightfish.sqlEngine.schema.MycatTable;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.fastsql.sql.repository.SchemaObject;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class MysqlTableExtractor extends MySqlASTVisitorAdapter {
    final Set<MycatTable> set = new HashSet<>();

    @Override
    public boolean visit(SQLExprTableSource x) {
        SchemaObject schemaObject = x.getSchemaObject();
        Objects.requireNonNull(schemaObject);
        MycatTable table = MycatSchemaManager.INSTANCE.getTable(schemaObject.getName(), schemaObject.getSchema().getName());
        if (table != null) {
            set.add(table);
        }
        return super.visit(x);
    }

    public Set<MycatTable> getSet() {
        return set;
    }
}