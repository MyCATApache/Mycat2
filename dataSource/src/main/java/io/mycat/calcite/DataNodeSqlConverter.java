package io.mycat.calcite;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Arrays;
import java.util.List;

public class DataNodeSqlConverter extends RelToSqlConverter {
    @Override
    public Result visit(TableScan e) {
        final List<String> qualifiedName = Arrays.asList(e.getTable().getQualifiedName().get(1).split("\\."));
        SqlIdentifier identifier = new SqlIdentifier (qualifiedName.subList(1,qualifiedName.size()),SqlParserPos.ZERO);
        return result(identifier, ImmutableList.of(Clause.FROM), e, null);
    }

    public DataNodeSqlConverter() {
        super(MysqlSqlDialect.DEFAULT);
    }
}