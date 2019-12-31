package io.mycat.sqlEngine.ast.extractor;

import io.mycat.sqlEngine.schema.StatementType;
import com.alibaba.fastsql.sql.ast.SQLStatement;

import java.util.Set;

public class Extractors {
    public final static StatementType getStatementType(SQLStatement sqlStatement) {
        MysqlStatementTypeExtractor mysqlStatementTypeExtractor = new MysqlStatementTypeExtractor();
        sqlStatement.accept(mysqlStatementTypeExtractor);
        return mysqlStatementTypeExtractor.getStatementType();
    }

    public final static Set<SchemaTablePair> getTables(String schemaName, SQLStatement statement) {
        MysqlTableExtractor tableExtractor = new MysqlTableExtractor(schemaName);
        statement.accept(tableExtractor);
        return tableExtractor.getDbSet();
    }
}