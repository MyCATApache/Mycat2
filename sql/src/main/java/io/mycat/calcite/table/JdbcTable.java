package io.mycat.calcite.table;

import io.mycat.calcite.BackEndTableInfo;
import io.mycat.calcite.MyCatResultSetEnumerable;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlDialect;
import com.google.common.collect.Lists;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.ResultSetEnumerable;
import org.apache.calcite.schema.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class JdbcTable implements TranslatableTable, ScannableTable, FilterableTable {
    private final String catalogName;
    private final String tableName;
    private final String schemaName;
    private DatabaseMetaData metaData;

    private ResultSet rs;
    private RelProtoDataType protoRowType;
    private Connection connection;
    private RowSignature rowSignature;

    private BackEndTableInfo[] info;
    private RexNode node;

    public JdbcTable(String schemaName, String tableName, BackEndTableInfo[] info) throws Exception {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.catalogName = "";
System.out.println("build table");
        // build the metadata for the table
        // TODO: build a metadata service
        Class.forName("com.mysql.cj.jdbc.Driver");
        connection = DriverManager
                .getConnection("jdbc:mysql://127.0.0.1:3306/test?serverTimezone=UTC",
                        "test","123456");

        this.info = info;
        metaData = connection.getMetaData();
        protoRowType = getRelDataType(metaData, null, info[0].schemaName, info[0].tableName);
    }

    RelProtoDataType getRelDataType(DatabaseMetaData metaData, String catalogName,
                                    String schemaName, String tableName) throws Exception {
        final ResultSet resultSet =
                metaData.getColumns("test", schemaName, tableName, null);

        // Temporary type factory, just for the duration of this method. Allowable
        // because we're creating a proto-type, not a type; before being used, the
        // proto-type will be copied into a real type factory.
        final RelDataTypeFactory typeFactory =
                new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
        RowSignature.Builder builder = RowSignature.builder();
        while (resultSet.next()) {
            final String columnName = resultSet.getString(4);
            final int dataType = resultSet.getInt(5);
            builder.add(columnName, JDBCType.valueOf(dataType) );
            final String typeString = resultSet.getString(6);
            final int precision;
            final int scale;
            switch (SqlType.valueOf(dataType)) {
                case TIMESTAMP:
                case TIME:
                    precision = resultSet.getInt(9); // SCALE
                    scale = 0;
                    break;
                default:
                    precision = resultSet.getInt(7); // SIZE
                    scale = resultSet.getInt(9); // SCALE
                    break;
            }
            RelDataType sqlType =
                    sqlType(typeFactory, dataType, precision, scale, typeString);
            boolean nullable = resultSet.getInt(11) != DatabaseMetaData.columnNoNulls;
            fieldInfo.add(columnName, sqlType).nullable(nullable);
        }
        resultSet.close();
        this.rowSignature = builder.build();
        return RelDataTypeImpl.proto(fieldInfo.build());
    }

    private RelDataType sqlType(RelDataTypeFactory typeFactory, int dataType,
                                int precision, int scale, String typeString) {
        // Fall back to ANY if type is unknown
        final SqlTypeName sqlTypeName =
                Util.first(SqlTypeName.getNameForJdbcType(dataType), SqlTypeName.ANY);
        switch (sqlTypeName) {
            case ARRAY:
                RelDataType component = null;
                if (typeString != null && typeString.endsWith(" ARRAY")) {
                    // E.g. hsqldb gives "INTEGER ARRAY", so we deduce the component type
                    // "INTEGER".
                    final String remaining = typeString.substring(0,
                            typeString.length() - " ARRAY".length());
                    component = parseTypeString(typeFactory, remaining);
                }
                if (component == null) {
                    component = typeFactory.createTypeWithNullability(
                            typeFactory.createSqlType(SqlTypeName.ANY), true);
                }
                return typeFactory.createArrayType(component, -1);
        }
        if (precision >= 0
                && scale >= 0
                && sqlTypeName.allowsPrecScale(true, true)) {
            return typeFactory.createSqlType(sqlTypeName, precision, scale);
        } else if (precision >= 0 && sqlTypeName.allowsPrecNoScale()) {
            return typeFactory.createSqlType(sqlTypeName, precision);
        } else {
            assert sqlTypeName.allowsNoPrecNoScale();
            return typeFactory.createSqlType(sqlTypeName);
        }
    }

    private RelDataType parseTypeString(RelDataTypeFactory typeFactory,
                                        String typeString) {
        int precision = -1;
        int scale = -1;
        int open = typeString.indexOf("(");
        if (open >= 0) {
            int close = typeString.indexOf(")", open);
            if (close >= 0) {
                String rest = typeString.substring(open + 1, close);
                typeString = typeString.substring(0, open);
                int comma = rest.indexOf(",");
                if (comma >= 0) {
                    precision = Integer.parseInt(rest.substring(0, comma));
                    scale = Integer.parseInt(rest.substring(comma));
                } else {
                    precision = Integer.parseInt(rest);
                }
            }
        }
        try {
            final SqlTypeName typeName = SqlTypeName.valueOf(typeString);
            return typeName.allowsPrecScale(true, true)
                    ? typeFactory.createSqlType(typeName, precision, scale)
                    : typeName.allowsPrecScale(true, false)
                    ? typeFactory.createSqlType(typeName, precision)
                    : typeFactory.createSqlType(typeName);
        } catch (IllegalArgumentException e) {
            return typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(SqlTypeName.ANY), true);
        }
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext toRelContext, RelOptTable relOptTable) {
        return LogicalTableScan.create(toRelContext.getCluster(), relOptTable);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
        try {
            return this.rowSignature.getRelDataType(relDataTypeFactory);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public Statistic getStatistic() {
        return Statistics.UNKNOWN;
    }

    @Override
    public Schema.TableType getJdbcTableType() {
        return Schema.TableType.TABLE;

    }

    @Override
    public boolean isRolledUp(String s) {
        return false;
    }

    @Override
    public boolean rolledUpColumnValidInsideAgg(String s, SqlCall sqlCall, SqlNode sqlNode, CalciteConnectionConfig calciteConnectionConfig) {
        return false;
    }


    private List<Pair<ColumnMetaData.Rep, Integer>> fieldClasses(
            final JavaTypeFactory typeFactory) {
        final RelDataType rowType = protoRowType.apply(typeFactory);
        return Lists.transform(rowType.getFieldList(), f -> {
            final RelDataType type = f.getType();
            final Class clazz = (Class) typeFactory.getJavaClass(type);
            final ColumnMetaData.Rep rep =
                    Util.first(ColumnMetaData.Rep.of(clazz),
                            ColumnMetaData.Rep.OBJECT);
            return Pair.of(rep, type.getSqlTypeName().getJdbcOrdinal());
        });
    }
    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        /*
        final JavaTypeFactory typeFactory = root.getTypeFactory();
                // JdbcUtils.ObjectArrayRowBuilder.factory(fieldClasses(typeFactory)));
        return ResultSetEnumerable.of(new SimpleDataSource(connection), "select * from test.test",
                JdbcUtils.ObjectArrayRowBuilder.factory(fieldClasses(typeFactory)));

         */
        return null;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> list) {
        final JavaTypeFactory typeFactory = root.getTypeFactory();
        String filterSql = null;
        // JdbcUtils.ObjectArrayRowBuilder.factory(fieldClasses(typeFactory)));
        /*
        System.out.println("filter push down");

        System.out.println("push down query " + sql);
        return ResultSetEnumerable.of(new SimpleDataSource(connection), sql,
                JdbcUtils.ObjectArrayRowBuilder.factory(fieldClasses(typeFactory)));

         */

        // check the filter push down

        if (!list.isEmpty()) {
            System.out.println(tableName + "-------------------------------------------");
            System.out.println(tableName);

            for (RexNode node : list) {
                System.out.println(node.toString());
                System.out.println(node.getKind().sql);
                System.out.println(node.getKind().name());
                this.node = node;
                RexCall call = (RexCall)node;
                RexInputRef left =(RexInputRef) call.getOperands().get(0);
                RexLiteral right = (RexLiteral) call.getOperands().get(1);
                System.out.println("left : " +left.getIndex());
                System.out.println("rigth : " + right.getValue2().toString());
                StringBuilder sb = new StringBuilder();
                sb.append(rowSignature.getRowOrder().get(left.getIndex()));
                sb.append(">");
                sb.append(right.getValue2().toString());

                filterSql = sb.toString();
                System.out.println(filterSql);
            }
            list.remove(node);

            System.out.println(tableName + "-------------------------------------------");
        }

        return new MyCatResultSetEnumerable<>(info, JdbcUtils.ObjectArrayRowBuilder.factory(fieldClasses(typeFactory)), filterSql);
    }

}

