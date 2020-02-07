package io.mycat.calcite;

import com.google.common.collect.ImmutableList;
import io.mycat.BackendTableInfo;
import io.mycat.calcite.logic.MycatLogicTable;
import io.mycat.calcite.logic.MycatPhysicalTable;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.materialize.SqlStatisticProvider;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.type.DelegatingTypeSystem;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.*;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.*;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.calcite.sql.type.InferTypes.FIRST_KNOWN;

public enum MycatCalciteContext implements Context {
    INSTANCE;
    final FrameworkConfig config;
    final CalciteConnectionConfig calciteConnectionConfig;
    final IdentityHashMap<Class, Object> map = new IdentityHashMap<>();
    final SqlParser.Config SQL_PARSER_CONFIG = SqlParser.configBuilder().setLex(Lex.MYSQL)
            .setConformance(SqlConformanceEnum.MYSQL_5)
            .setCaseSensitive(false).build();
    final SqlToRelConverter.Config sqlToRelConverterConfig = SqlToRelConverter.configBuilder()
            .withConfig(SqlToRelConverter.Config.DEFAULT)
            .withTrimUnusedFields(true)
            .withRelBuilderFactory(new RelBuilderFactory() {
                @Override
                public RelBuilder create(RelOptCluster cluster, RelOptSchema schema) {
                    return new MycatRelBuilder(MycatCalciteContext.INSTANCE, cluster, schema);
                }
            }).build();

    public MycatTypeSystem TypeSystem = new MycatTypeSystem();
    public JavaTypeFactoryImpl TypeFactory = new JavaTypeFactoryImpl(TypeSystem);

    volatile SchemaPlus ROOT_SCHEMA;

    public void flash() {
        SchemaPlus rootSchema = Frameworks.createRootSchema(false);
        SchemaPlus dataNodes = rootSchema.add(MetadataManager.DATA_NODES, new AbstractSchema());
        for (Map.Entry<String, ConcurrentHashMap<String, MetadataManager.LogicTable>> stringConcurrentHashMapEntry : MetadataManager.INSTANCE.logicTableMap.entrySet()) {
            SchemaPlus schemaPlus = rootSchema.add(stringConcurrentHashMapEntry.getKey(), new AbstractSchema());
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
        this.ROOT_SCHEMA = rootSchema;
    }

    MycatCalciteContext() {
        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
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

        Driver driver = new Driver();//触发驱动注册
        final String charset = "UTF-8";
        System.setProperty("saffron.default.charset", charset);
        System.setProperty("saffron.default.nationalcharset", charset);
        System.setProperty("calcite.default.charset", charset);
        System.setProperty("saffron.default.collat​​ion.tableName", charset + "$ en_US");


        map.put(FrameworkConfig.class, config);
        map.put(CalciteConnectionConfig.class, calciteConnectionConfig);
        map.put(RelDataTypeSystem.class, TypeSystem);
        map.put(MycatTypeSystem.class, TypeSystem);
        map.put(SqlParser.Config.class, SQL_PARSER_CONFIG);
        flash();
    }

    private CalciteConnectionConfig connectionConfig() {
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
                return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE),true);
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

    public final SqlBinaryOperator DIVIDE = new SqlBinaryOperator(
            "/",
            SqlKind.DIVIDE,
            60,
            true,
            ReturnTypes.QUOTIENT_NULLABLE,
            (callBinding, returnType, operandTypes) -> {
                FIRST_KNOWN.inferOperandTypes(callBinding, returnType, operandTypes);
                for (RelDataType operandType : operandTypes) {
                    SqlTypeFamily family = operandType.getSqlTypeName().getFamily();
                    if (SqlTypeFamily.NUMERIC.equals(family)) {
                        RelDataType sqlType = TypeFactory.createTypeWithNullability(TypeFactory.createSqlType(SqlTypeName.DOUBLE), true);
                        for (int i = 0; i < operandTypes.length; i++) {
                            operandTypes[i] = sqlType;
                        }
                        return;
                    }
                }
            },
            OperandTypes.DIVISION_OPERATOR) {
        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            return super.deriveType(validator, scope, call);
        }
    };

    public FrameworkConfig create(String defaultSchemaName) {
        SchemaPlus subSchema = ROOT_SCHEMA.getSubSchema(defaultSchemaName);
        return new FrameworkConfig() {
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
                return MycatCalciteContext.INSTANCE.config.getExecutor();
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
                return TypeSystem;
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
        };
    }

    ;

}