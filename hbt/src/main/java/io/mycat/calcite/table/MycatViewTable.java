/**
 * Copyright (C) <2021>  <chen junwen>
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
package io.mycat.calcite.table;

import com.google.common.collect.ImmutableList;
import io.mycat.ViewHandler;
import lombok.Getter;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Junwen Chen
 **/
@Getter
public class MycatViewTable extends AbstractTable implements TranslatableTable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatViewTable.class);

    String schemaName;
    String tableName;
    RelDataType relDataType;
    String sql;


    public MycatViewTable(ViewHandler handler, RelDataType relDataType) {
        this.schemaName = handler.getSchemaName();
        this.tableName = handler.getViewName();
        this.relDataType = relDataType;
        this.sql = handler.getViewSql();
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        try {
            final RelRoot root =
                    context.expandView(relOptTable.getRowType(), sql, ImmutableList.of(getSchemaName()), ImmutableList.of(getSchemaName(),getTableName()));
            final RelNode rel = RelOptUtil.createCastRel(root.rel, relOptTable.getRowType(), true);
            // Expand any views
            final RelNode rel2 = rel.accept(
                    new RelShuttleImpl() {
                        @Override public RelNode visit(TableScan scan) {
                            final RelOptTable table = scan.getTable();
                            final TranslatableTable translatableTable =
                                    table.unwrap(TranslatableTable.class);
                            if (translatableTable != null) {
                                return translatableTable.toRel(context, table);
                            }
                            return super.visit(scan);
                        }
                    });
            return  root.withRel(rel2).rel;
        } catch (Exception e) {
            throw new RuntimeException("Error while parsing view definition: "
                    + sql, e);
        }
    }

    @Override
    public Schema.TableType getJdbcTableType() {
        return Schema.TableType.VIEW;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return relDataType;
    }
}