/**
 * Copyright (C) <2019>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.calcite;

import com.google.common.collect.ImmutableList;
import io.mycat.SchemaInfo;
import io.mycat.calcite.logic.MycatPhysicalTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Arrays;

/**
 * @author Junwen Chen
 **/
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

//    /** @see #dispatch */
//    public Result visit(MycatTransientSQLTableScan scan) {
//        return scan.implement();
//    }

    public Result implement(RelNode node) {
        return dispatch(node);
    }
}