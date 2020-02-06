package io.mycat.calcite;

import com.google.common.collect.ImmutableList;
import io.mycat.SchemaInfo;
import io.mycat.calcite.logic.MycatPhysicalTable;
import io.mycat.calcite.relBuilder.MycatTransientSQLTableScan;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Arrays;

public class MycatImplementor extends RelToSqlConverter {
    @Override
    public Result visit(TableScan e) {
        try {
            MycatPhysicalTable physicalTable = e.getTable().unwrap(MycatPhysicalTable.class);
            if (physicalTable != null) {
                SchemaInfo schemaInfo = physicalTable.getBackendTableInfo().getSchemaInfo();
                SqlIdentifier identifier = new SqlIdentifier(Arrays.asList(schemaInfo.getTargetSchema(),schemaInfo.getTargetTable()), SqlParserPos.ZERO);
                return result(identifier, ImmutableList.of(Clause.FROM), e, null);
            } else {
                return super.visit(e);
            }
        } catch (Throwable e1) {
            return null;
        }

    }

//    public static String toString(RelNode node) {
//        try {
//            MycatImplementor dataNodeSqlConverter = new MycatImplementor();
//            SqlImplementor.Result visit = dataNodeSqlConverter.visitChild(0, node);
//            SqlNode sqlNode = visit.asStatement();
//            return sqlNode.toSqlString(MysqlSqlDialect.DEFAULT).getSql();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return null;
//    }

    public MycatImplementor(SqlDialect dialect) {
        super(dialect);
    }

    /** @see #dispatch */
    public Result visit(MycatTransientSQLTableScan scan) {
        return scan.implement();
    }

    public Result implement(RelNode node) {
        return dispatch(node);
    }
}