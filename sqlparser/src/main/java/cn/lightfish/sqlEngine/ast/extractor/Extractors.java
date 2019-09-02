package cn.lightfish.sqlEngine.ast.extractor;

import cn.lightfish.sqlEngine.schema.MycatTable;
import cn.lightfish.sqlEngine.schema.StatementType;
import com.alibaba.fastsql.sql.ast.SQLStatement;

import java.util.Set;

public class Extractors {
    public final static StatementType getStatementType(SQLStatement sqlStatement) {
        MysqlStatementTypeExtractor mysqlStatementTypeExtractor = new MysqlStatementTypeExtractor();
        sqlStatement.accept(mysqlStatementTypeExtractor);
        return mysqlStatementTypeExtractor.getStatementType();
    }

    public final static Set<MycatTable> getTables(SQLStatement statement) {
        MysqlTableExtractor tableExtractor = new MysqlTableExtractor();
        statement.accept(tableExtractor);
        return tableExtractor.getSet();
    }
}