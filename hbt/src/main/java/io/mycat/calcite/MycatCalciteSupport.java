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

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.RowIteratorUtil;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.prepare.MycatCalcitePlanner;
import io.mycat.calcite.resultset.CalciteRowMetaData;
import io.mycat.calcite.table.SingeTargetSQLTable;
import io.mycat.hbt.ColumnInfoRowMetaData;
import io.mycat.hbt.RelNodeConvertor;
import io.mycat.hbt.TextConvertor;
import io.mycat.hbt.ast.base.Schema;
import io.mycat.upondb.MycatDBContext;
import io.mycat.util.Explains;
import org.apache.calcite.config.*;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.type.DelegatingTypeSystem;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeCoercionRule;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.implicit.TypeCoercionFactory;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Junwen Chen
 **/
public enum MycatCalciteSupport implements Context {
    INSTANCE;
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatCalciteSupport.class);
    public static final Driver DRIVER = new Driver();//触发驱动注册
    public final FrameworkConfig config;
    public final CalciteConnectionConfig calciteConnectionConfig;
    public final IdentityHashMap<Class, Object> map = new IdentityHashMap<>();
    /*

    new SqlParserImplFactory() {
                @Override
                @SneakyThrows
                public SqlAbstractParserImpl getParser(Reader stream) {
                    String string = CharStreams.toString(stream);
                    SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(string);
                    SqlParserImplFactory factory = SqlParserImpl.FACTORY;
                    CalciteMySqlNodeVisitor calciteMySqlNodeVisitor = new CalciteMySqlNodeVisitor();
                    sqlStatement.accept(calciteMySqlNodeVisitor);
                    SqlNode sqlNode = calciteMySqlNodeVisitor.getSqlNode();
                    return new SqlAbstractParserImpl() {
                    };
                }
            }
     */
    public final SqlParser.Config SQL_PARSER_CONFIG = SqlParser.configBuilder().setLex(Lex.MYSQL)
            .setConformance(SqlConformanceEnum.MYSQL_5)
            .setCaseSensitive(false).build();
    public MycatTypeSystem TypeSystem = new MycatTypeSystem();
    public JavaTypeFactoryImpl TypeFactory = new JavaTypeFactoryImpl(TypeSystem){
        @Override
        public Charset getDefaultCharset() {
            return StandardCharsets.UTF_8;
        }
    };
    public RexBuilder RexBuilder = new RexBuilder(TypeFactory);
    public RelBuilderFactory relBuilderFactory = new RelBuilderFactory() {
        @Override
        public RelBuilder create(RelOptCluster cluster, RelOptSchema schema) {
            return new MycatRelBuilder(MycatCalciteSupport.INSTANCE, cluster, schema);
        }
    };

    public final SqlToRelConverter.Config sqlToRelConverterConfig = SqlToRelConverter.configBuilder()
            .withConfig(SqlToRelConverter.Config.DEFAULT)
            .withTrimUnusedFields(true)
            .withInSubQueryThreshold(Integer.MAX_VALUE)
            .withRelBuilderFactory(relBuilderFactory).build();

    public final SqlValidator.Config getValidatorConfig() {
       return SqlValidator.Config.DEFAULT;
//                .withSqlConformance(calciteConnectionConfig.conformance());
    }
//    new SqlValidator.Config() {
//        @Override
//        public boolean callRewrite() {
//            return false;
//        }
//
//        @Override
//        public SqlValidator.Config withCallRewrite(boolean rewrite) {
//            return this;
//        }
//
//        @Override
//        public NullCollation defaultNullCollation() {
//            return NullCollation.HIGH;
//        }
//
//        @Override
//        public SqlValidator.Config withDefaultNullCollation(NullCollation nullCollation) {
//            return this;
//        }
//
//        @Override
//        public boolean columnReferenceExpansion() {
//            return false;
//        }
//
//        @Override
//        public SqlValidator.Config withColumnReferenceExpansion(boolean expand) {
//            return this;
//        }
//
//        @Override
//        public boolean identifierExpansion() {
//            return false;
//        }
//
//        @Override
//        public SqlValidator.Config withIdentifierExpansion(boolean expand) {
//            return this;
//        }
//
//        @Override
//        public boolean lenientOperatorLookup() {
//            return true;
//        }
//
//        @Override
//        public SqlValidator.Config withLenientOperatorLookup(boolean lenient) {
//            return this;
//        }
//
//        @Override
//        public boolean typeCoercionEnabled() {
//            return false;
//        }
//
//        @Override
//        public SqlValidator.Config withTypeCoercionEnabled(boolean enabled) {
//            return this;
//        }
//
//        @Override
//        public TypeCoercionFactory typeCoercionFactory() {
//            return SqlValidator.Config.DEFAULT.typeCoercionFactory();
//        }
//
//        @Override
//        public SqlValidator.Config withTypeCoercionFactory(TypeCoercionFactory factory) {
//            return this;
//        }
//
//        @Override
//        public SqlTypeCoercionRule typeCoercionRules() {
//            return SqlValidator.Config.DEFAULT.typeCoercionRules();
//        }
//
//        @Override
//        public SqlValidator.Config withTypeCoercionRules(SqlTypeCoercionRule rules) {
//            return this;
//        }
//
//        @Override
//        public SqlConformance sqlConformance() {
//            return MycatCalciteSupport.INSTANCE.calciteConnectionConfig.conformance();
//        }
//
//        @Override
//        public SqlValidator.Config withSqlConformance(SqlConformance conformance) {
//            return this;
//        }
//    };


    public MycatCalciteDataContext create(MycatDBContext uponDBContext) {
        return new MycatCalciteDataContext(uponDBContext);
    }

    MycatCalciteSupport() {

        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
        configBuilder.parserConfig(SQL_PARSER_CONFIG);
        configBuilder.typeSystem(TypeSystem);
        configBuilder.sqlToRelConverterConfig(sqlToRelConverterConfig);
        SqlStdOperatorTable instance = SqlStdOperatorTable.instance();
        instance.init();
        configBuilder.operatorTable(new SqlOperatorTable() {
            final HashMap<String, SqlOperator> map = new HashMap<>();
            final HashMap<String, SqlOperator> build = new HashMap<>();
            {
                map.put("IFNULL", SqlStdOperatorTable.COALESCE);
                build.put("SUBSTR", SqlStdOperatorTable.SUBSTRING);
                build.put("CURDATE", SqlStdOperatorTable.CURRENT_DATE);
                build.put("CURRENT_DATE",SqlStdOperatorTable.CURRENT_DATE);
                build.put("NOW", SqlStdOperatorTable.LOCALTIMESTAMP);
                build.put("LOG", SqlStdOperatorTable.LOG10);
                build.put("PI",SqlStdOperatorTable.PI);
                build.put("POW",SqlStdOperatorTable.POWER);
                for (Map.Entry<String, SqlOperator> stringSqlOperatorEntry : build.entrySet()) {
                    map.put(stringSqlOperatorEntry.getKey().toUpperCase(),stringSqlOperatorEntry.getValue());
                    map.put(stringSqlOperatorEntry.getKey().toLowerCase(),stringSqlOperatorEntry.getValue());
                }

            }

            @Override
            public void lookupOperatorOverloads(SqlIdentifier opName, SqlFunctionCategory category, SqlSyntax syntax, List<SqlOperator> operatorList, SqlNameMatcher nameMatcher) {
                SqlOperator sqlOperator = map.get(opName.getSimple());
                if (sqlOperator != null) {
                    operatorList.add(sqlOperator);
                }
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
                return (T) MycatCalciteSupport.INSTANCE.TypeSystem;
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

    public SqlAbstractParserImpl createSqlParser(Reader reader) {
        return config.getParserConfig().parserFactory().getParser(reader);
    }

    public String convertToHBTText(RelNode relNode, MycatCalciteDataContext dataContext) {
        MycatCalcitePlanner planner = createPlanner(dataContext);
        Schema schema;
        try {
            schema  = RelNodeConvertor.convertRelNode(planner.convertToMycatRel(relNode));
        }catch (Throwable e){
            String message = "hbt无法生成";
            LOGGER.warn(message,e);
            return message;
        }
        return convertToHBTText(schema);
    }

    public String convertToHBTText(Schema schema) {
        return TextConvertor.dumpExplain(schema);
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
//            SqlTypeFamily a = type1.getSqlTypeName().getFamily();
//            SqlTypeFamily b = type2.getSqlTypeName().getFamily();
//            if (SqlTypeFamily.NUMERIC.equals(a) || SqlTypeFamily.NUMERIC.equals(b)) {
//                return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
//            }
//            RelDataType relDataType = super.deriveDecimalDivideType(typeFactory, type1, type2);
//            if (typeFactory.createUnknownType().equals(relDataType)) {
//                return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
//            }
            return super.deriveDecimalDivideType(typeFactory, type1, type2);
        }
    }

    public enum MycatStandardConvertletTable implements SqlRexConvertletTable {
        INSTANCE;

        @Override
        public SqlRexConvertlet get(SqlCall call) {
            SqlRexConvertlet sqlRexConvertlet = StandardConvertletTable.INSTANCE.get(call);
//            if (call.getKind() == SqlKind.DIVIDE) {
//                return (cx, call1) -> {
//                    //,mysql除法返回浮点型
//                    RexNode rexNode = sqlRexConvertlet.convertCall(cx, call1);
//                    final RexBuilder rexBuilder = cx.getRexBuilder();
//                    RelDataType sqlType = cx.getTypeFactory().createSqlType(SqlTypeName.DOUBLE);
//                    ImmutableList<RexNode> operands = ((RexCall) rexNode).operands;
//                    RexNode a = operands.get(0);
//                    RexNode b = operands.get(1);
//                    if (b.getType().getSqlTypeName().getFamily() == SqlTypeFamily.NUMERIC) {
//                        return rexBuilder.makeCall(((RexCall) rexNode).getOperator(), rexBuilder.makeCast(sqlType, a), b);
//                    } else {
//                        return rexNode;
//                    }
//                };
//            }

            return sqlRexConvertlet;
        }
    }

    public MycatCalcitePlanner createPlanner(MycatCalciteDataContext dataContext) {
        Objects.requireNonNull(dataContext);
        return new MycatCalcitePlanner(dataContext);
    }

    public MycatCalcitePlanner createPlanner(MycatDBContext dataContext) {
        Objects.requireNonNull(dataContext);
        return new MycatCalcitePlanner(create(dataContext));
    }

    public CalciteConnectionConfig getCalciteConnectionConfig() {
        return calciteConnectionConfig;
    }

    public String convertToSql(RelNode input, SqlDialect dialect, boolean forUpdate) {
        MycatImplementor mycatImplementor = new MycatImplementor(dialect);
        input= RelOptUtil.createCastRel(input,input.getRowType(),true);
        SqlImplementor.Result implement = mycatImplementor.implement(input);
        SqlNode sqlNode = implement.asStatement();
        String sql = sqlNode.toSqlString(dialect, false).getSql();
        sql = sql.trim();
        sql = sql.replaceAll("\r", " ");
        sql = sql.replaceAll("\n", " ");
        return sql + (forUpdate ? " for update" : "");
    }

    public String convertToMycatRelNodeText(RelNode node, MycatCalciteDataContext dataContext) {
        final StringWriter sw = new StringWriter();
        final RelWriter planWriter = new RelWriterImpl(new PrintWriter(sw), SqlExplainLevel.EXPPLAN_ATTRIBUTES, false);
        RelNode relNode = MycatCalciteSupport.INSTANCE.createPlanner(dataContext).convertToMycatRel(node);
        relNode.explain(planWriter);
        return sw.toString();
    }


    public String dumpMetaData(RelDataType mycatRowMetaData) {
        return RowIteratorUtil.dumpColumnInfo(convertToRowIterator(mycatRowMetaData));
    }

    public RowBaseIterator convertToRowIterator(RelDataType mycatRowMetaData) {
        return ColumnInfoRowMetaData.INSTANCE.convertToRowIterator(new CalciteRowMetaData(mycatRowMetaData.getFieldList()));
    }

    public String dumpMetaData(MycatRowMetaData mycatRowMetaData) {
        return RowIteratorUtil.dumpColumnInfo(ColumnInfoRowMetaData.INSTANCE.convertToRowIterator(mycatRowMetaData));
    }


    public String convertToHBTText(List<SingeTargetSQLTable> tables) {
        return tables.stream()
                .map(preComputationSQLTable ->
                        new Explains.PrepareCompute(preComputationSQLTable.getTargetName(), preComputationSQLTable.getSql(), preComputationSQLTable.params()).toString()).collect(Collectors.joining(",\n"));
    }
}