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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.RowIteratorUtil;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.resultset.CalciteRowMetaData;
import io.mycat.calcite.sqlfunction.CRC32Function;
import io.mycat.calcite.sqlfunction.cmpfunction.StrictEqualFunction;
import io.mycat.calcite.sqlfunction.datefunction.*;
import io.mycat.calcite.sqlfunction.mathfunction.Log2Function;
import io.mycat.calcite.sqlfunction.mathfunction.LogFunction;
import io.mycat.calcite.sqlfunction.mathfunction.RandFunction;
import io.mycat.calcite.sqlfunction.mathfunction.TruncateFunction;
import io.mycat.calcite.sqlfunction.stringfunction.*;
import io.mycat.calcite.table.SingeTargetSQLTable;
import io.mycat.hbt.ColumnInfoRowMetaData;
import io.mycat.hbt.RelNodeConvertor;
import io.mycat.hbt.TextConvertor;
import io.mycat.hbt.ast.base.Schema;
import io.mycat.util.Explains;
import io.mycat.util.NameMap;
import lombok.SneakyThrows;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.mycat.*;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.type.DelegatingTypeSystem;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.*;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.implicit.TypeCoercion;
import org.apache.calcite.sql.validate.implicit.TypeCoercionFactory;
import org.apache.calcite.sql.validate.implicit.TypeCoercionImpl;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.BuiltInMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Junwen Chen
 **/
public enum MycatCalciteSupport implements Context {
    INSTANCE;
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatCalciteSupport.class);
    public static final Driver DRIVER = new Driver();//触发驱动注册
    public static final FrameworkConfig config;
    public static final CalciteConnectionConfig calciteConnectionConfig;
    public static final IdentityHashMap<Class, Object> map = new IdentityHashMap<>();
    public static final NameMap<Class> functions = new NameMap<>();

    static {
//        functions.put("date_format", DateFormatFunction.class)
//                .put("UNIX_TIMESTAMP", UnixTimestampFunction.class)
//                .put("concat", ConcatFunction.class)
//                .put("CONCAT_WS", ConcatWSFunction.class)
//                .put("PI", PiFunction.class)
//                .put("CONV", CONVFunction.class)
//                .put("crc32", CRC32Function.class)
//                .put("log", LOGFunction.class)
//                .put("log2", LOG2Function.class)
//                .put("log10", LOG10Function.class)
//                .put("|", BitWiseOrFunction.class)
//                .put("bin", BinFunction.class)
//                .put("BIT_LENGTH", BitLengthFunction.class)
//                .put("CHAR", CharFunction.class)
//                .put("LAST_INSERT_ID", LastInsertIdFunction.class);

        fixCalcite();

    }

//    public static final SqlParser.Config SQL_PARSER_CONFIG = SqlParser.configBuilder().setLex(Lex.MYSQL)
//            .setConformance(SqlConformanceEnum.MYSQL_5)
//            .setCaseSensitive(false).build();
    public static final MycatTypeSystem TypeSystem = new MycatTypeSystem();
    public static final RelDataTypeFactory TypeFactory = new MycatRelDataTypeFactory(TypeSystem);
    public static RexBuilder RexBuilder = new RexBuilder(TypeFactory);
    public static RelBuilderFactory relBuilderFactory = new RelBuilderFactory() {
        @Override
        public RelBuilder create(RelOptCluster cluster, RelOptSchema schema) {
            return new MycatRelBuilder(MycatCalciteSupport.INSTANCE, cluster, schema);
        }
    };

    public static final SqlToRelConverter.Config sqlToRelConverterConfig = SqlToRelConverter.configBuilder()
            .withTrimUnusedFields(true)
            .withInSubQueryThreshold(Integer.MAX_VALUE)
            .withRelBuilderConfigTransform(config -> config.withSimplify(false))
            .withRelBuilderFactory(relBuilderFactory).build();

    public final SqlValidator.Config getValidatorConfig() {
        SqlTypeMappingRule instance = SqlTypeMappingRules.instance(true);
        Map<SqlTypeName, ImmutableSet<SqlTypeName>> typeMapping = instance.getTypeMapping();


        final Map<SqlTypeName, ImmutableSet<SqlTypeName>> map = new HashMap(typeMapping);
        map.put(SqlTypeName.BOOLEAN,
                ImmutableSet.<SqlTypeName>builder()
                        .addAll(typeMapping.get(SqlTypeName.BOOLEAN))
                        .addAll(SqlTypeName.NUMERIC_TYPES).build());

        for (SqlTypeName numericType : SqlTypeName.NUMERIC_TYPES) {
            map.put(numericType,
                    ImmutableSet.<SqlTypeName>builder()
                            .addAll(typeMapping.get(numericType))
                            .addAll(SqlTypeName.BOOLEAN_TYPES).build());
        }

        SqlTypeCoercionRule instance1 = SqlTypeCoercionRule.instance(map);
        return SqlValidator.Config.DEFAULT.withSqlConformance(calciteConnectionConfig.conformance())
                .withTypeCoercionEnabled(true)
                .withTypeCoercionRules(instance1).withLenientOperatorLookup(true)
                .withTypeCoercionFactory(new TypeCoercionFactory() {

                    @Override
                    public TypeCoercion create(RelDataTypeFactory typeFactory, SqlValidator validator) {
                        return new TypeCoercionImpl(typeFactory, validator) {
                            @Override
                            public RelDataType implicitCast(RelDataType in, SqlTypeFamily expected) {
                                if (in.getSqlTypeName() == SqlTypeName.BOOLEAN && SqlTypeFamily.NUMERIC == expected) {
                                    return super.factory.createSqlType(SqlTypeName.TINYINT);
                                }
                                if (SqlTypeName.STRING_TYPES.contains(in.getSqlTypeName()) && expected == SqlTypeFamily.DATE) {
                                    return super.factory.createSqlType(SqlTypeName.DATE);
                                }
                                if (SqlTypeName.STRING_TYPES.contains(in.getSqlTypeName()) && expected == SqlTypeFamily.DATETIME) {
                                    return super.factory.createSqlType(SqlTypeName.TIMESTAMP);
                                }
                                if (in.getSqlTypeName() == SqlTypeName.TIMESTAMP && expected == SqlTypeFamily.NUMERIC) {
                                    return super.factory.createSqlType(SqlTypeName.DOUBLE);
                                }
                                if (in.getSqlTypeName() == SqlTypeName.TIME && expected == SqlTypeFamily.NUMERIC) {
                                    return super.factory.createSqlType(SqlTypeName.DOUBLE);
                                }
                                if (in.getSqlTypeName() == SqlTypeName.DATE && expected == SqlTypeFamily.NUMERIC) {
                                    return super.factory.createSqlType(SqlTypeName.BIGINT);
                                }
                                return super.implicitCast(in, expected);
                            }
                        };
                    }
                });
//                .withSqlConformance(calciteConnectionConfig.conformance());
    }

    static {
        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
//        configBuilder.parserConfig(SQL_PARSER_CONFIG);
        configBuilder.typeSystem(TypeSystem);
        configBuilder.sqlToRelConverterConfig(sqlToRelConverterConfig);
        SqlStdOperatorTable instance = SqlStdOperatorTable.instance();
        instance.init();
        configBuilder.operatorTable(new SqlOperatorTable() {
            final Multimap<String, SqlOperator> map = HashMultimap.create();
            final Multimap<String, SqlOperator> build = HashMultimap.create();

            {
                try {
                    map.put("IFNULL", SqlStdOperatorTable.COALESCE);
                    build.put("SUBSTR", SqlStdOperatorTable.SUBSTRING);
                    build.put("CURDATE", CurDateFunction.INSTANCE);
                    build.put("CURRENT_DATE", CurDateFunction.INSTANCE);
//                    build.put("NOW", SqlStdOperatorTable.LOCALTIMESTAMP);
//                    build.put("LOG", SqlStdOperatorTable.LOG);
                    build.put("PI", SqlStdOperatorTable.PI);
                    build.put("POW", SqlStdOperatorTable.POWER);
                    build.put("concat", ConcatFunction.INSTANCE);
                    build.put("<=>", StrictEqualFunction.INSTANCE);
                    build.put("regexp", RegexpFunction.INSTANCE);
                    build.put("concat_ws", ConcatWsFunction.INSTANCE);
                    build.put("regexp_instr", RegexpInstrFunction.INSTANCE);
                    build.put("regexp_replace", RegexpReplaceFunction.INSTANCE);
                    build.put("not regexp", NotRegexpFunction.INSTANCE);

                    Arrays.asList(BitLengthFunction.INSTANCE,
                            BinFunction.INSTANCE,
                            AsciiFunction.INSTANCE,
                            RegexpSubstrFunction.INSTANCE,
                            BinaryFunction.INSTANCE,
                            CharLengthFunction.INSTANCE,
                            ChrFunction.INSTANCE,
                            ConvertFunction.INSTANCE,
                            EltFunction.INSTANCE,
                            ExportSetFunction.INSTANCE,
                            ExtractValueFunction.INSTANCE,
                            FieldFunction.INSTANCE,
                            FindInSetFunction.INSTANCE,
                            FormatFunction.INSTANCE,
                            FromBase64Function.INSTANCE,
                            HexFunction.INSTANCE,
                            InsertFunction.INSTANCE,
                            InstrFunction.INSTANCE,
                            LowerFunction.INSTANCE,
                            LeftFunction.INSTANCE,
                            LengthFunction.INSTANCE,
                            loadFileFunction.INSTANCE,
                            LocateFunction.INSTANCE,
                            Locate2Function.INSTANCE,
                            LpadFunction.INSTANCE,
                            LtrimFunction.INSTANCE,
                            MakeSetFunction.INSTANCE,
                            MidFunction.INSTANCE,
                            RepeatFunction.INSTANCE,
                            OrdFunction.INSTANCE,
                            PositionFunction.INSTANCE,
                            QuoteFunction.INSTANCE,
                            ReverseFunction.INSTANCE,
                            RightFunction.INSTANCE,
                            RpadFunction.INSTANCE,
                            RtrimFunction.INSTANCE,
                            SoundexFunction.INSTANCE,
                            SpaceFunction.INSTANCE,
                            StrCmpFunction.INSTANCE,
                            SubStringFunction.INSTANCE,
                            CharFunction.INSTANCE,
                            StringIndexFunction.INSTANCE,
                            ToBase64Function.INSTANCE,
                            TrimFunction.INSTANCE,
                            UpperFunction.INSTANCE,
                            UncompressedLengthFunction.INSTANCE,
                            UnhexFunction.INSTANCE,
                            UpdateXMLFunction.INSTANCE,
                            WeightStringFunction.INSTANCE,
                            /////////////////////////////////////////
                            AddDateFunction.INSTANCE,
//                            DateAddFunction.INSTANCE,
                            RexImpTable.AddTimeFunction.INSTANCE,
                            ConvertTzFunction.INSTANCE,
                            CurDateFunction.INSTANCE,
                            DateDiffFunction.INSTANCE,
                            DateFormatFunction.INSTANCE,
                            DateFormat2Function.INSTANCE,
                            StringToTimestampFunction.INSTANCE,
                            RexImpTable.DateAddFunction.INSTANCE,
                            RexImpTable.DateSubFunction.INSTANCE,
                            RexImpTable.ExtractFunction.INSTANCE,
                            DayOfWeekFunction.INSTANCE,
                            FromDaysFunction.INSTANCE,
                            HourFunction.INSTANCE,
                            FromUnixTimeFormatFunction.INSTANCE,
                            FromUnixTimeFunction.INSTANCE,
                            GetFormatFunction.INSTANCE,
                            LastDayFunction.INSTANCE,
                            MakeDateFunction.INSTANCE,
                            MakeTimeFunction.INSTANCE,
                            MicrosecondFunction.INSTANCE,
                            MinuteFunction.INSTANCE,
                            MonthFunction.INSTANCE,
                            MonthNameFunction.INSTANCE,
                            PeriodAddFunction.INSTANCE,
                            PeriodDiffFunction.INSTANCE,
                            QuarterFunction.INSTANCE,
                            SecondFunction.INSTANCE,
                            SecToTimeFunction.INSTANCE,

                            SecToDateFunction.INSTANCE,
                            SubTimeFunction.INSTANCE,
                            SysDateFunction.INSTANCE,
                            TimeDiff2Function.INSTANCE,
                            TimeDiffFunction.INSTANCE,
                            TimeDiff3Function.INSTANCE,
                            TimeFunction.INSTANCE,
                            TimestampFunction.INSTANCE,
                            Timestamp2Function.INSTANCE,
                            Timestamp3Function.INSTANCE,
                            TimestampComposeFunction.INSTANCE,
                            TimestampAddFunction.INSTANCE,
                            TimestampDiffFunction.INSTANCE,
                            TimeFormatFunction.INSTANCE,
                            TimeToSecFunction.INSTANCE,
                            ToDaysFunction.INSTANCE,
                            ToSecondsFunction.INSTANCE,
                            UtcDateFunction.INSTANCE,
                            UtcTimeFunction.INSTANCE,
                            UtcTimestampFunction.INSTANCE,
                            WeekFunction.INSTANCE,
                            WeekDayFunction.INSTANCE,
                            WeekOfYearFunction.INSTANCE,
                            YearFunction.INSTANCE,
                            YearWeekFunction.INSTANCE,
                            MycatDatabaseFunction.INSTANCE,
                            MycatSessionValueFunction.INSTANCE,
                            MycatGlobalValueFunction.INSTANCE,
                            MycatUserValueFunction.INSTANCE,
                            MycatVersionFunction.INSTANCE,
                            MycatLastInsertIdFunction.INSTANCE,
                            MycatConnectionIdFunction.INSTANCE,
                            MycatCurrentUserFunction.INSTANCE,
                            MycatUserFunction.INSTANCE,
                            ConvFunction.INSTANCE,
                            BitOrFunction.INSTANCE,
                            TrimLeadingFunction.INSTANCE,
                            TrimBothFunction.INSTANCE,
                            TrimTrailingFunction.INSTANCE,
                            CRC32Function.INSTANCE,
                            LogFunction.INSTANCE,
                            Log2Function.INSTANCE,
                            RandFunction.INSTANCE,
                            TruncateFunction.INSTANCE,
                            DaynameFunction.INSTANCE,
                            DayOfYearFunction.INSTANCE
                    ).forEach(i -> build.put(i.getName(), i));
                    build.put("CHARACTER_LENGTH", CharLengthFunction.INSTANCE);
                    build.put("LCASE", LowerFunction.INSTANCE);
                    build.put("UCASE", UpperFunction.INSTANCE);
                    build.put("LENGTHB", LengthFunction.INSTANCE);
                    build.put("OCTET_LENGTH", LengthFunction.INSTANCE);
                    build.put("SUBSTR", SubStringFunction.INSTANCE);
                    build.put("CURRENT_DATE", CurDateFunction.INSTANCE);
                    build.put("CURTIME", CurTimeFunction.INSTANCE);
                    build.put("CURRENT_TIME", CurTimeFunction.INSTANCE);

                    build.put("NOW", NowFunction.INSTANCE);
                    build.put("CURRENT_TIMESTAMP", NowFunction.INSTANCE);
                    build.put("LOCALTIME", NowFunction.INSTANCE);
                    build.put("LOCALTIMESTAMP", NowFunction.INSTANCE);

                    build.put("DATE", DateFunction.INSTANCE);
                    build.put("DAY", DayOfMonthFunction.INSTANCE);
                    build.put("DAYOFMONTH", DayOfMonthFunction.INSTANCE);
                    build.put("UNIX_TIMESTAMP", UnixTimestampFunction.INSTANCE);

                    ////////////////////////////////////////////////

                    for (Map.Entry<String, SqlOperator> stringSqlOperatorEntry : build.entries()) {
                        map.put(stringSqlOperatorEntry.getKey().toUpperCase(), stringSqlOperatorEntry.getValue());
                        map.put(stringSqlOperatorEntry.getKey().toLowerCase(), stringSqlOperatorEntry.getValue());
                    }
                } catch (Throwable e) {
                    LOGGER.warn("", e);
                }

            }

            @Override
            public void lookupOperatorOverloads(SqlIdentifier opName, SqlFunctionCategory category, SqlSyntax syntax, List<SqlOperator> operatorList, SqlNameMatcher nameMatcher) {
                Collection<SqlOperator> sqlOperator = map.get(opName.getSimple().toUpperCase());
                if (sqlOperator != null) {
                    operatorList.addAll(sqlOperator);
                    if (sqlOperator == ConvertFunction.INSTANCE) {//fix bug
                        // class org.apache.calcite.sql.fun.SqlConvertFunction: CONVERT is broken.
                        return;
                    }
                }
                instance.lookupOperatorOverloads(opName, category, syntax, operatorList, nameMatcher);
            }

            @Override
            public List<SqlOperator> getOperatorList() {
                return instance.getOperatorList();
            }
        });
        configBuilder.convertletTable(MycatStandardConvertletTable.INSTANCE);

        config = configBuilder.context(MycatCalciteSupport.INSTANCE).build();
        calciteConnectionConfig = connectionConfig();

        map.put(FrameworkConfig.class, config);
        map.put(CalciteConnectionConfig.class, calciteConnectionConfig);
        map.put(RelDataTypeSystem.class, TypeSystem);
        map.put(MycatTypeSystem.class, TypeSystem);
//        map.put(SqlParser.Config.class, SQL_PARSER_CONFIG);
        map.put(RexExecutor.class, MycatRexExecutor.INSTANCE);
        map.put(RelBuilder.Config.class, RelBuilder.Config.DEFAULT);

    }

//    public MycatCalciteDataContext create(MycatDBContext uponDBContext) {
//        return new MycatCalciteDataContext(uponDBContext);
//    }

    @SneakyThrows
    MycatCalciteSupport() {
        try {

            Class<? extends BuiltInMethod> aClass = BuiltInMethod.STRING_TO_TIMESTAMP.getClass();
            System.out.println();
//            Class<? extends RexImpTable> aClass = RexImpTable.class;
//            Field mapField = aClass.getDeclaredField("map");
//            mapField.setAccessible(true);
//            Map<SqlOperator, RexImpTable.RexCallImplementor> o = (Map<SqlOperator, RexImpTable.RexCallImplementor>) mapField.get(RexImpTable.INSTANCE);
//            System.out.println(o);
//
//            Method defineMethod = aClass.getDeclaredMethod("defineMethod", SqlOperator.class, Method.class,
//                    NullPolicy.class);
//            defineMethod.setAccessible(true);
//
//
//            Map<SqlOperator, RexImpTable.RexCallImplementor> res = new ConcurrentHashMap<SqlOperator, RexImpTable.RexCallImplementor>() {
//
//                @SneakyThrows
//                @Override
//                public RexImpTable.RexCallImplementor get(Object key) {
//                    RexImpTable.RexCallImplementor rexCallImplementor = super.get(key);
//                    if (rexCallImplementor != null) {
//                        return rexCallImplementor;
//                    }
//                    SqlOperator k = (SqlOperator) key;
//                    String name = k.getName();
//                    switch (name.toLowerCase()){
//                        case "regexp":{
//                            ScalarFunctionImpl implementor = (ScalarFunctionImpl)RegexpFunction.scalarFunction;
//                            defineMethod.invoke(RexImpTable.INSTANCE, key, implementor.method, NullPolicy.ANY);
//                          break;
//                        }
//                    }
//                    return get(key);
//                }
//            };
//            res.putAll(o);
//            mapField.set(RexImpTable.INSTANCE, res);
//
//            Field instanceField = aClass.getDeclaredField("INSTANCE");
//            instanceField.setAccessible(true);

//            Constructor<?> declaredConstructor = aClass.getDeclaredConstructors()[0];
//            declaredConstructor.setAccessible(true);

        } catch (Throwable e) {
            System.out.println(e);
        }

//        }catch (Throwable e){
//          System.err.println(e);
//        }
    }

    private static void fixCalcite() {
//
//        Map<SqlOperator, RexImpTable.RexCallImplementor> map = MycatClassResolver.forceStaticGet(RexImpTable.class, RexImpTable.INSTANCE, "map");
//        map.put(SqlStdOperatorTable.CAST, new MycatCastImplementor());
        //            Class<? extends RexImpTable> aClass = RexImpTable.class;
//            Field mapField = aClass.getDeclaredField("map");
//            mapField.setAccessible(true);
//            Map<SqlOperator, RexImpTable.RexCallImplementor> o = (Map<SqlOperator, RexImpTable.RexCallImplementor>) mapField.get(RexImpTable.INSTANCE);
//            System.out.println(o);
    }

    private static CalciteConnectionConfig connectionConfig() {
        final String charset = "UTF-8";
        System.setProperty("saffron.default.charset", charset);
        System.setProperty("saffron.default.nationalcharset", charset);
        System.setProperty("calcite.default.charset", charset);
        System.setProperty("saffron.default.collation.tableName", charset + "$ en_US");
        Properties properties = new Properties();
        properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
                String.valueOf(false));
        properties.setProperty(CalciteConnectionProperty.CONFORMANCE.camelName(),
                String.valueOf(SqlConformanceEnum.MYSQL_5));

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

    public String convertToHBTText(RelNode relNode) {
        Schema schema;
        try {
            schema = RelNodeConvertor.convertRelNode(relNode);
        } catch (Throwable e) {
            String message = "hbt无法生成";
            LOGGER.warn(message, e);
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

//    public MycatCalcitePlanner createPlanner(MycatCalciteDataContext dataContext) {
//        Objects.requireNonNull(dataContext);
//        return new MycatCalcitePlanner(dataContext);
//    }

//    public MycatCalcitePlanner createPlanner(MycatDBContext dataContext) {
//        Objects.requireNonNull(dataContext);
//        return new MycatCalcitePlanner(create(dataContext));
//    }

    public CalciteConnectionConfig getCalciteConnectionConfig() {
        return calciteConnectionConfig;
    }

    public SqlString convertToSql(RelNode input, SqlDialect dialect, boolean forUpdate) {
        return convertToSql(input, dialect, forUpdate, Collections.emptyList());
    }

    public SqlString convertToSql(RelNode input, SqlDialect dialect, boolean forUpdate, List<Object> params) {
        MycatImplementor mycatImplementor = new MycatImplementor(MycatSqlDialect.DEFAULT, params);
        SqlImplementor.Result implement = mycatImplementor.implement(input);
        SqlNode sqlNode = implement.asStatement();
        if (forUpdate) {
            sqlNode = SqlForUpdate.OPERATOR.createCall(SqlParserPos.ZERO, sqlNode);
        }
        final SqlWriterConfig config = SqlPrettyWriter.config().withDialect(dialect)
                .withAlwaysUseParentheses(true)
                .withSelectListItemsOnSeparateLines(false)
                .withUpdateSetListNewline(false)
                .withQuoteAllIdentifiers(true);//mysql fun name should not wrapper quote
        SqlPrettyWriter writer = new SqlPrettyWriter(config) {
            @Override
            public void dynamicParam(int index) {
                super.dynamicParam(index);
            }

            @Override
            public void identifier(String name, boolean quoted) {
                super.identifier(name, quoted);
            }

            @Override
            public Frame startFunCall(String funName) {
                return super.startFunCall(funName);
            }
        };
        sqlNode.unparse(writer, 0, 0);
        return writer.toSqlString();

    }

    public String convertToMycatRelNodeText(RelNode node) {
        final StringWriter sw = new StringWriter();
        final RelWriter planWriter = new RelWriterImpl(new PrintWriter(sw), SqlExplainLevel.EXPPLAN_ATTRIBUTES, false);
        node.explain(planWriter);
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