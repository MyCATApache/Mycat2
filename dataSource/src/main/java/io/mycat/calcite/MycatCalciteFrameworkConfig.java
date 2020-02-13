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
import org.apache.calcite.materialize.SqlStatisticProvider;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Program;

import java.util.List;

/**
 * @author Junwen Chen
 **/
public class MycatCalciteFrameworkConfig implements FrameworkConfig {
    final SchemaPlus subSchema;

    public MycatCalciteFrameworkConfig(SchemaPlus subSchema) {
        this.subSchema = subSchema;
    }

    @Override
    public SqlParser.Config getParserConfig() {
        return MycatCalciteContext.INSTANCE.config.getParserConfig();
    }

    @Override
    public SqlToRelConverter.Config getSqlToRelConverterConfig() {
        return MycatCalciteContext.INSTANCE.config.getSqlToRelConverterConfig();
    }

    @Override
    public SchemaPlus getDefaultSchema() {
        return subSchema;
    }

    @Override
    public RexExecutor getExecutor() {
        return new RexExecutor() {
            @Override
            public void reduce(RexBuilder rexBuilder, List<RexNode> constExps, List<RexNode> reducedValues) {
                RexExecutor executor = MycatCalciteContext.INSTANCE.config.getExecutor();
                executor.reduce(rexBuilder, constExps, reducedValues);
            }
        };
    }

    @Override
    public ImmutableList<Program> getPrograms() {
        return MycatCalciteContext.INSTANCE.config.getPrograms();
    }

    @Override
    public SqlOperatorTable getOperatorTable() {
        return MycatCalciteContext.INSTANCE.config.getOperatorTable();
    }

    @Override
    public RelOptCostFactory getCostFactory() {
        return MycatCalciteContext.INSTANCE.config.getCostFactory();
    }

    @Override
    public ImmutableList<RelTraitDef> getTraitDefs() {
        return MycatCalciteContext.INSTANCE.config.getTraitDefs();
    }

    @Override
    public SqlRexConvertletTable getConvertletTable() {
        return MycatCalciteContext.INSTANCE.config.getConvertletTable();
    }

    @Override
    public Context getContext() {
        return MycatCalciteContext.INSTANCE;
    }

    @Override
    public RelDataTypeSystem getTypeSystem() {
        return MycatCalciteContext.INSTANCE.TypeSystem;
    }

    @Override
    public boolean isEvolveLattice() {
        return MycatCalciteContext.INSTANCE.config.isEvolveLattice();
    }

    @Override
    public SqlStatisticProvider getStatisticProvider() {
        return MycatCalciteContext.INSTANCE.config.getStatisticProvider();
    }

    @Override
    public RelOptTable.ViewExpander getViewExpander() {
        return MycatCalciteContext.INSTANCE.config.getViewExpander();
    }
}