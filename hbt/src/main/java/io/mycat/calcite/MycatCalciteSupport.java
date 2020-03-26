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
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.RowIteratorUtil;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.prepare.MycatCalcitePlanner;
import io.mycat.calcite.resultset.CalciteRowMetaData;
import io.mycat.calcite.table.PreComputationSQLTable;
import io.mycat.hbt.ColumnInfoRowMetaData;
import io.mycat.hbt.RelNodeConvertor;
import io.mycat.hbt.TextConvertor;
import io.mycat.hbt.ast.base.Schema;
import io.mycat.upondb.MycatDBContext;
import io.mycat.util.Explains;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.type.DelegatingTypeSystem;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
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

import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * @author Junwen Chen
 **/
public enum MycatCalciteSupport implements Context {
    INSTANCE;

    public static final Driver DRIVER = new Driver();//触发驱动注册
   public final FrameworkConfig config;
    public final CalciteConnectionConfig calciteConnectionConfig;
    public   final IdentityHashMap<Class, Object> map = new IdentityHashMap<>();
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
    public    final SqlParser.Config SQL_PARSER_CONFIG = SqlParser.configBuilder().setLex(Lex.MYSQL)
            .setConformance(SqlConformanceEnum.MYSQL_5)
            .setCaseSensitive(false).build();
    public MycatTypeSystem TypeSystem = new MycatTypeSystem();
    public JavaTypeFactoryImpl TypeFactory = new JavaTypeFactoryImpl(TypeSystem);
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
            .withRelBuilderFactory(relBuilderFactory).build();



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
        Schema schema = RelNodeConvertor.convertRelNode(planner.convertToMycatRel(relNode));
        return convertToHBTText(schema);
    }

    public String convertToHBTText(Schema schema) {
        return   TextConvertor.dumpExplain(schema);
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

    public String convertToSql(RelNode input, SqlDialect dialect,boolean forUpdate) {
        MycatImplementor mycatImplementor = new MycatImplementor(dialect);
        SqlImplementor.Result implement = mycatImplementor.implement(input);
        SqlNode sqlNode =implement.asStatement();
        String sql = sqlNode.toSqlString(dialect, false).getSql();
        SqlImplementor.Result implement2 = mycatImplementor.implement(input);
        sql = sql.replaceAll("\r", " ");
        sql = sql.replaceAll("\n", " ");
        return sql+(forUpdate?" for update":"");
    }

    public String convertToMycatRelNodeText(RelNode node,MycatCalciteDataContext dataContext) {
        final StringWriter sw = new StringWriter();
        final RelWriter planWriter = new RelWriterImpl(new PrintWriter(sw), SqlExplainLevel.EXPPLAN_ATTRIBUTES, false);
        RelNode relNode = MycatCalciteSupport.INSTANCE.createPlanner(dataContext).convertToMycatRel(node);
        relNode.explain(planWriter);
        return sw.toString();
    }


    public  String dumpMetaData(RelDataType mycatRowMetaData) {
        return RowIteratorUtil.dumpColumnInfo(convertToRowIterator(mycatRowMetaData));
    }

    public  RowBaseIterator convertToRowIterator(RelDataType mycatRowMetaData) {
        return ColumnInfoRowMetaData.INSTANCE.convertToRowIterator(new CalciteRowMetaData(mycatRowMetaData.getFieldList()));
    }
    public  String dumpMetaData(MycatRowMetaData mycatRowMetaData) {
        return RowIteratorUtil.dumpColumnInfo(ColumnInfoRowMetaData.INSTANCE.convertToRowIterator(mycatRowMetaData));
    }


    public String convertToHBTText(List<PreComputationSQLTable> tables) {
        return tables.stream()
                .map(preComputationSQLTable ->
                        new Explains.PrepareCompute(preComputationSQLTable.getTargetName(), preComputationSQLTable.getSql(), preComputationSQLTable.params()).toString()).collect(Collectors.joining(",\n"));
    }
}