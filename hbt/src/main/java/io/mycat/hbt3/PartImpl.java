/**
 * Copyright (C) <2020>  <chen junwen>
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
package io.mycat.hbt3;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.MycatSqlDialect;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlString;

import java.util.List;


public class PartImpl implements Part {
    int schemaIndex;
    int tableIndex;
    int mysqlIndex;

    public PartImpl(int datasourceSize, int schemaIndex, int tableIndex) {
        this.schemaIndex = schemaIndex;
        this.tableIndex = tableIndex;
        this.mysqlIndex = schemaIndex % datasourceSize;
    }

    @Override
    public int getMysqlIndex() {
        return mysqlIndex;
    }

    @Override
    public int getSchemaIndex() {
        return schemaIndex;
    }

    @Override
    public SqlString getSql(RelNode node) {
        SqlDialect dialect = MycatSqlDialect.DEFAULT;
        ToSQL toSQL = new ToSQL(schemaIndex, tableIndex, dialect);

        SqlImplementor.Result result = toSQL.visitRoot(node);
        SqlNode sqlNode = result.asStatement();
        return sqlNode.toSqlString(dialect, false);
    }

    public class ToSQL extends RelToSqlConverter {
        int schemaIndex;
        int tableIndex;

        public ToSQL(int schemaIndex, int tableIndex, SqlDialect dialect) {
            super(dialect);
            this.schemaIndex = schemaIndex;
            this.tableIndex = tableIndex;
        }

        @Override
        public Result visit(TableScan e) {
            if (e.getTable() != null) {
                AbstractMycatTable mycatTable = e.getTable().unwrap(AbstractMycatTable.class);
                if (mycatTable != null) {
                    if (mycatTable.isSharding()) {
                        String schemaName = getBackendSchemaName(mycatTable);
                        String tableName = getBackendTableName(mycatTable);
                        final List<String> qualifiedName = ImmutableList.of(schemaName, tableName);
                        SqlIdentifier sqlIdentifier = new SqlIdentifier(qualifiedName, SqlParserPos.ZERO);
                        return result(sqlIdentifier, ImmutableList.of(Clause.FROM), e, null);
                    }
                }
            }
            return super.visit(e);
        }
    }

    public String getBackendTableName(AbstractMycatTable mycatTable) {
        return mycatTable.getTableName() + "_" + tableIndex;
    }

    public String getBackendSchemaName(AbstractMycatTable mycatTable) {
        return mycatTable.getSchemaName() + "_" + schemaIndex;
    }

}