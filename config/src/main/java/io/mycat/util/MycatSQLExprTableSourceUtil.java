package io.mycat.util;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import org.jetbrains.annotations.NotNull;

public class MycatSQLExprTableSourceUtil {
    public static SQLExprTableSource createSQLExprTableSource(String schemaName, String tableName) {
        SQLExprTableSource sqlExprTableSource = new SQLExprTableSource();
        return setSqlExprTableSource(schemaName, tableName, sqlExprTableSource);
    }

    @NotNull
    public static SQLExprTableSource setSqlExprTableSource(String schemaName, String tableName, SQLExprTableSource sqlExprTableSource) {
        if (tableName != null) {
            tableName = SQLUtils.normalize(tableName);
            sqlExprTableSource.setSimpleName("`" + tableName + "`");//顺序不能颠倒
        }
        if (schemaName != null) {
            schemaName = SQLUtils.normalize(schemaName);
            sqlExprTableSource.setSchema("`" + schemaName + "`");
        }
        return sqlExprTableSource;
    }
    public static SQLExprTableSource setSchema(String schemaName, SQLExprTableSource sqlExprTableSource) {
        if (schemaName != null) {
            schemaName = SQLUtils.normalize(schemaName);
            sqlExprTableSource.setSchema("`" + schemaName + "`");
        }
        return sqlExprTableSource;
    }
}
