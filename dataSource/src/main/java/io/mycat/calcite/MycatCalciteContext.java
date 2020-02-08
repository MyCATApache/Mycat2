package io.mycat.calcite;

import com.google.common.collect.ImmutableList;
import io.mycat.BackendTableInfo;
import io.mycat.calcite.logic.MycatLogicTable;
import io.mycat.calcite.logic.MycatPhysicalTable;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.type.DelegatingTypeSystem;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.*;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public enum MycatCalciteContext implements Context {
    INSTANCE;
    final FrameworkConfig config;
    final CalciteConnectionConfig calciteConnectionConfig;
    final IdentityHashMap<Class, Object> map = new IdentityHashMap<>();
    final SqlParser.Config SQL_PARSER_CONFIG = SqlParser.configBuilder().setLex(Lex.MYSQL)
            .setConformance(SqlConformanceEnum.MYSQL_5)
            .setCaseSensitive(false).build();

    public MycatTypeSystem TypeSystem = new MycatTypeSystem();
    public JavaTypeFactoryImpl TypeFactory = new JavaTypeFactoryImpl(TypeSystem);
    public RexBuilder RexBuilder = new RexBuilder(TypeFactory);
    public RelBuilderFactory relBuilderFactory = new RelBuilderFactory() {
        @Override
        public RelBuilder create(RelOptCluster cluster, RelOptSchema schema) {
            return new MycatRelBuilder(MycatCalciteContext.INSTANCE, cluster, schema);
        }
    };

    final SqlToRelConverter.Config sqlToRelConverterConfig = SqlToRelConverter.configBuilder()
            .withConfig(SqlToRelConverter.Config.DEFAULT)
            .withTrimUnusedFields(true)
            .withRelBuilderFactory(relBuilderFactory).build();

    volatile SchemaPlus ROOT_SCHEMA;

    public void flash() {
        SchemaPlus plus = CalciteSchema.createRootSchema(true).plus();
        SchemaPlus dataNodes = plus.add(MetadataManager.DATA_NODES, new AbstractSchema());
        for (Map.Entry<String, ConcurrentHashMap<String, MetadataManager.LogicTable>> stringConcurrentHashMapEntry : MetadataManager.INSTANCE.logicTableMap.entrySet()) {
            SchemaPlus schemaPlus = plus.add(stringConcurrentHashMapEntry.getKey(), new AbstractSchema());
            for (Map.Entry<String, MetadataManager.LogicTable> entry : stringConcurrentHashMapEntry.getValue().entrySet()) {
                MetadataManager.LogicTable logicTable = entry.getValue();
                MycatLogicTable mycatLogicTable = new MycatLogicTable(logicTable);
                schemaPlus.add(entry.getKey(), mycatLogicTable);

                for (BackendTableInfo backend : logicTable.getBackends()) {
                    String uniqueName = backend.getUniqueName();
                    MycatPhysicalTable mycatPhysicalTable = new MycatPhysicalTable(mycatLogicTable, backend);
                    dataNodes.add(uniqueName, mycatPhysicalTable);
                }
            }
        }
        ROOT_SCHEMA = plus;
    }

    MycatCalciteContext() {

        Driver driver = new Driver();//触发驱动注册
        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
        configBuilder.parserConfig(SQL_PARSER_CONFIG);
        configBuilder.typeSystem(TypeSystem);
        configBuilder.sqlToRelConverterConfig(sqlToRelConverterConfig);

        SqlStdOperatorTable instance = SqlStdOperatorTable.instance();
        instance.init();
        configBuilder.operatorTable(new SqlOperatorTable() {
            @Override
            public void lookupOperatorOverloads(SqlIdentifier opName, SqlFunctionCategory category, SqlSyntax syntax, List<SqlOperator> operatorList, SqlNameMatcher nameMatcher) {
                instance.lookupOperatorOverloads(opName, category, syntax, operatorList, nameMatcher);
            }

            @Override
            public List<SqlOperator> getOperatorList() {
                return instance.getOperatorList();
            }
        });
        configBuilder.convertletTable(MycatStandardConvertletTable.INSTANCE);

        this.config = configBuilder.context(this).build();
        this.calciteConnectionConfig = connectionConfig();

        map.put(FrameworkConfig.class, config);
        map.put(CalciteConnectionConfig.class, calciteConnectionConfig);
        map.put(RelDataTypeSystem.class, TypeSystem);
        map.put(MycatTypeSystem.class, TypeSystem);
        map.put(SqlParser.Config.class, SQL_PARSER_CONFIG);
        map.put(RexExecutor.class, RexUtil.EXECUTOR);
        flash();
    }

    private CalciteConnectionConfig connectionConfig() {
        final String charset = "UTF-8";
        System.setProperty("saffron.default.charset", charset);
        System.setProperty("saffron.default.nationalcharset", charset);
        System.setProperty("calcite.default.charset", charset);
        System.setProperty("saffron.default.collat​​ion.tableName", charset + "$ en_US");
        Properties properties = new Properties();
        properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
                String.valueOf(false));
        properties.setProperty(CalciteConnectionProperty.CONFORMANCE.camelName(),
                String.valueOf(config.getParserConfig().conformance()));

        return new CalciteConnectionConfigImpl(properties) {
            @Override
            public <T> T typeSystem(Class<T> typeSystemClass, T defaultTypeSystem) {
                return (T) MycatCalciteContext.INSTANCE.TypeSystem;
            }

            @Override
            public SqlConformance conformance() {
                return SqlConformanceEnum.MYSQL_5;
            }

        };
    }

    @Override
    public <C> C unwrap(Class<C> aClass) {
        Object o = map.get(aClass);
        return (C) o;
    }


    public static class MycatTypeSystem extends DelegatingTypeSystem {


        public MycatTypeSystem() {
            super(RelDataTypeSystem.DEFAULT);
        }

        @Override
        public RelDataType deriveAvgAggType(RelDataTypeFactory typeFactory, RelDataType argumentType) {
            SqlTypeFamily a = argumentType.getSqlTypeName().getFamily();
            if (SqlTypeFamily.NUMERIC.equals(a)) {
                return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
            }
            return super.deriveAvgAggType(typeFactory, argumentType);
        }

        @Override
        public RelDataType deriveDecimalDivideType(RelDataTypeFactory typeFactory, RelDataType type1, RelDataType type2) {
            SqlTypeFamily a = type1.getSqlTypeName().getFamily();
            SqlTypeFamily b = type2.getSqlTypeName().getFamily();
            if (SqlTypeFamily.NUMERIC.equals(a) || SqlTypeFamily.NUMERIC.equals(b)) {
                return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
            }
            RelDataType relDataType = super.deriveDecimalDivideType(typeFactory, type1, type2);
            if (typeFactory.createUnknownType().equals(relDataType)) {
                return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
            }
            return super.deriveDecimalDivideType(typeFactory, type1, type2);
        }
    }

    public enum MycatStandardConvertletTable implements SqlRexConvertletTable {
        INSTANCE;

        @Override
        public SqlRexConvertlet get(SqlCall call) {
            SqlRexConvertlet sqlRexConvertlet = StandardConvertletTable.INSTANCE.get(call);
            if (call.getKind() == SqlKind.DIVIDE) {
                return (cx, call1) -> {
                    //,mysql除法返回浮点型
                    RexNode rexNode = sqlRexConvertlet.convertCall(cx, call1);
                    final RexBuilder rexBuilder = cx.getRexBuilder();
                    RelDataType sqlType = cx.getTypeFactory().createSqlType(SqlTypeName.DOUBLE);
                    ImmutableList<RexNode> operands = ((RexCall) rexNode).operands;
                    RexNode a = operands.get(0);
                    RexNode b = operands.get(1);
                    if (b.getType().getSqlTypeName().getFamily() == SqlTypeFamily.NUMERIC) {
                        return rexBuilder.makeCall(((RexCall) rexNode).getOperator(), rexBuilder.makeCast(sqlType, a), b);
                    } else {
                        return rexNode;
                    }
                };
            }

            return sqlRexConvertlet;
        }
    }

    public MycatCalciteFrameworkConfig createFrameworkConfig(String defaultSchemaName) {
        return new MycatCalciteFrameworkConfig(ROOT_SCHEMA.getSubSchema(defaultSchemaName));
    }

    public MycatCalcitePlanner createPlanner(String defaultSchemaName) throws SqlParseException {
        SchemaPlus currentRootSchema = ROOT_SCHEMA;
        return new MycatCalcitePlanner(currentRootSchema,defaultSchemaName);
    }


    ;

}