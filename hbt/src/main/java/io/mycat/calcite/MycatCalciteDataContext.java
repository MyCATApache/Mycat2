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

import org.apache.calcite.rel.type.RelDataTypeFactory;

/**
 * @author Junwen Chen
 **/

public class MycatCalciteDataContext {

    public RelDataTypeFactory getTypeFactory() {
        return MycatCalciteSupport.INSTANCE.TypeFactory;
    }

//    @Override
//    public SqlParser.Config getParserConfig() {
//        return MycatCalciteSupport.INSTANCE.config.getParserConfig();
//    }
//
//    @Override
//    public SqlValidator.Config getSqlValidatorConfig() {
//        return SqlValidator.Config.DEFAULT;
//    }
//
//    @Override
//    public SqlToRelConverter.Config getSqlToRelConverterConfig() {
//        return MycatCalciteSupport.INSTANCE.config.getSqlToRelConverterConfig();
//    }
//
//    @Override
//    public SchemaPlus getDefaultSchema() {
//        String schema = uponDBContext.getSchema();
//        if (schema == null) {
//            return getRootSchema();
//        } else {
//            return getRootSchema().getSubSchema(schema);
//        }
//    }
//
//    @Override
//    public RexExecutor getExecutor() {
//        return (rexBuilder, constExps, reducedValues) -> {
//            RexExecutor executor = MycatCalciteSupport.INSTANCE.config.getExecutor();
//            if (executor != null) {
//                executor.reduce(rexBuilder, constExps, reducedValues);
//            }
//        };
//    }
//
//    @Override
//    public ImmutableList<Program> getPrograms() {
//        return MycatCalciteSupport.INSTANCE.config.getPrograms();
//    }
//
//    @Override
//    public SqlOperatorTable getOperatorTable() {
//        return MycatCalciteSupport.INSTANCE.config.getOperatorTable();
//    }
//
//    @Override
//    public RelOptCostFactory getCostFactory() {
//        return MycatCalciteSupport.INSTANCE.config.getCostFactory();
//    }
//
//    @Override
//    public ImmutableList<RelTraitDef> getTraitDefs() {
//        return MycatCalciteSupport.INSTANCE.config.getTraitDefs();
//    }
//
//    @Override
//    public SqlRexConvertletTable getConvertletTable() {
//        return MycatCalciteSupport.INSTANCE.config.getConvertletTable();
//    }
//
//    @Override
//    public Context getContext() {
//        return MycatCalciteSupport.INSTANCE;
//    }
//
//    @Override
//    public RelDataTypeSystem getTypeSystem() {
//        return MycatCalciteSupport.INSTANCE.TypeSystem;
//    }
//
//    @Override
//    public boolean isEvolveLattice() {
//        return MycatCalciteSupport.INSTANCE.config.isEvolveLattice();
//    }
//
//    @Override
//    public SqlStatisticProvider getStatisticProvider() {
//        return MycatCalciteSupport.INSTANCE.config.getStatisticProvider();
//    }
//
//    @Override
//    public RelOptTable.ViewExpander getViewExpander() {
//        return MycatCalciteSupport.INSTANCE.config.getViewExpander();
//    }

}