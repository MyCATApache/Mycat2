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
import com.google.common.collect.ImmutableMap;
import io.mycat.api.collector.UpdateRowIteratorResponse;
import io.mycat.calcite.table.MycatLogicTable;
import io.mycat.calcite.table.MycatPhysicalTable;
import io.mycat.calcite.table.PreComputationSQLTable;
import io.mycat.metadata.LogicTable;
import io.mycat.upondb.Components;
import io.mycat.upondb.MycatDBClientBased;
import io.mycat.upondb.MycatDBContext;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.materialize.SqlStatisticProvider;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Program;

import java.util.*;

/**
 * @author Junwen Chen
 **/

public class MycatCalciteDataContext implements DataContext, FrameworkConfig {
    private final MycatDBContext uponDBContext;
    private Map<String, Object> variables;

    public MycatCalciteDataContext(MycatDBContext uponDBContext) {
        this.uponDBContext = uponDBContext;
    }

    private ImmutableMap<String, Object> getCalciteLocalVariable() {
        final long time = System.currentTimeMillis();
        TimeZone timeZone = TimeZone.getDefault();
        final long localOffset = timeZone.getOffset(time);
        final long currentOffset = localOffset;
        final String systemUser = System.getProperty("user.name");
        final String user = "sa";
        final Locale locale = Locale.getDefault();
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        builder.put(Variable.UTC_TIMESTAMP.camelName, time)
                .put(Variable.CURRENT_TIMESTAMP.camelName, time + currentOffset)
                .put(Variable.LOCAL_TIMESTAMP.camelName, time + localOffset)
                .put(Variable.TIME_ZONE.camelName, timeZone)
                .put(Variable.USER.camelName, user)
                .put(Variable.SYSTEM_USER.camelName, systemUser)
                .put(Variable.LOCALE.camelName, locale)
                .put(Variable.STDIN.camelName, System.in)
                .put(Variable.STDOUT.camelName, System.out)
                .put(Variable.STDERR.camelName, System.err)
                .put(Variable.CANCEL_FLAG.camelName, uponDBContext.cancelFlag());
        return builder.build();
    }

    public SchemaPlus getRootSchema() {
        SchemaPlus component = uponDBContext.getUponDBSharedServer().getComponent(Components.SCHEMA, aByte -> getSchema(uponDBContext));
        return component;
    }

    public JavaTypeFactory getTypeFactory() {
        return MycatCalciteSupport.INSTANCE.TypeFactory;
    }

    public QueryProvider getQueryProvider() {
        return null;
    }

    public Object get(String name) {
        Object o = uponDBContext.get(name);
        if (o == null) {
            Map<String, Object> variables = uponDBContext.variables();
            if (variables != null) {
                Object o1 = variables.get(name);
                if (o1 != null) {
                    return o1;
                }
            }
        }
        if (variables == null) {
            variables = getCalciteLocalVariable();
        }
        return variables.get(name);
    }


    public void preComputation(PreComputationSQLTable preComputationSQLTable) {
        List<Object[]> objects = preComputationSQLTable.scan(this).toList();
        uponDBContext.cache(preComputationSQLTable, preComputationSQLTable.getTargetName(),preComputationSQLTable.getSql(),
                Collections.emptyList(),objects);
    }

    public Enumerable<Object[]> removePreComputation(PreComputationSQLTable preComputationSQLTable) {
        Object o = uponDBContext.removeCache(preComputationSQLTable,preComputationSQLTable.getTargetName(),preComputationSQLTable.getSql(),Collections.emptyList());
        if (o != null) {
            return Linq4j.asEnumerable((List<Object[]>) o);
        } else {
            return null;
        }
    }

    public UpdateRowIteratorResponse getUpdateRowIterator(String targetName, List<String> sqls) {
        return uponDBContext.update(targetName, sqls);
    }

    public static SchemaPlus getSchema(MycatDBClientBased based) {
        SchemaPlus plus = CalciteSchema.createRootSchema(true).plus();
        Map<String, Map<String, LogicTable>> logicTableMap = based.config();
        for (Map.Entry<String, Map<String, LogicTable>> stringConcurrentHashMapEntry : logicTableMap.entrySet()) {
            SchemaPlus schemaPlus = plus.add(stringConcurrentHashMapEntry.getKey(), new AbstractSchema());
            for (Map.Entry<String, LogicTable> entry : stringConcurrentHashMapEntry.getValue().entrySet()) {
                LogicTable logicTable = entry.getValue();
                MycatLogicTable mycatLogicTable = new MycatLogicTable(logicTable);
                schemaPlus.add(entry.getKey(), mycatLogicTable);
            }
        }
        return plus;
    }

    @Override
    public SqlParser.Config getParserConfig() {
        return MycatCalciteSupport.INSTANCE.config.getParserConfig();
    }

    @Override
    public SqlToRelConverter.Config getSqlToRelConverterConfig() {
        return MycatCalciteSupport.INSTANCE.config.getSqlToRelConverterConfig();
    }

    @Override
    public SchemaPlus getDefaultSchema() {
        String schema = uponDBContext.getSchema();
        if (schema == null) {
            return getRootSchema();
        } else {
            return getRootSchema().getSubSchema(schema);
        }
    }

    @Override
    public RexExecutor getExecutor() {
        return (rexBuilder, constExps, reducedValues) -> {
            RexExecutor executor = MycatCalciteSupport.INSTANCE.config.getExecutor();
            executor.reduce(rexBuilder, constExps, reducedValues);
        };
    }

    @Override
    public ImmutableList<Program> getPrograms() {
        return MycatCalciteSupport.INSTANCE.config.getPrograms();
    }

    @Override
    public SqlOperatorTable getOperatorTable() {
        return MycatCalciteSupport.INSTANCE.config.getOperatorTable();
    }

    @Override
    public RelOptCostFactory getCostFactory() {
        return MycatCalciteSupport.INSTANCE.config.getCostFactory();
    }

    @Override
    public ImmutableList<RelTraitDef> getTraitDefs() {
        return MycatCalciteSupport.INSTANCE.config.getTraitDefs();
    }

    @Override
    public SqlRexConvertletTable getConvertletTable() {
        return MycatCalciteSupport.INSTANCE.config.getConvertletTable();
    }

    @Override
    public Context getContext() {
        return MycatCalciteSupport.INSTANCE;
    }

    @Override
    public RelDataTypeSystem getTypeSystem() {
        return MycatCalciteSupport.INSTANCE.TypeSystem;
    }

    @Override
    public boolean isEvolveLattice() {
        return MycatCalciteSupport.INSTANCE.config.isEvolveLattice();
    }

    @Override
    public SqlStatisticProvider getStatisticProvider() {
        return MycatCalciteSupport.INSTANCE.config.getStatisticProvider();
    }

    @Override
    public RelOptTable.ViewExpander getViewExpander() {
        return MycatCalciteSupport.INSTANCE.config.getViewExpander();
    }


    public MycatDBContext getUponDBContext() {
        return uponDBContext;
    }

    public MycatLogicTable getLogicTable(String targetName, String schema, String table) {
        String uniqueName = targetName + "." + schema + "." + table;
        SchemaPlus rootSchema = getRootSchema();
        for (String subSchemaName : rootSchema.getSubSchemaNames()) {
            SchemaPlus subSchema = rootSchema.getSubSchema(subSchemaName);
            Set<String> tableNames = subSchema.getTableNames();
            for (String tableName : tableNames) {
                Table table1 = subSchema.getTable(tableName);
                if (table1 instanceof MycatLogicTable) {
                    Map<String, MycatPhysicalTable> dataNodeMap = ((MycatLogicTable) table1).getDataNodeMap();
                    if (dataNodeMap.containsKey(uniqueName)) {
                        return (MycatLogicTable)table1;
                    }
                }
            }
        }
        return null;
    }
}