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
import io.mycat.calcite.table.MycatSQLTableScan;
import io.mycat.calcite.table.MycatTransientSQLTableScan;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Junwen Chen
 **/
public class MycatRelBuilder extends RelBuilder {
    int id = 0;
    public MycatRelBuilder(Context context, RelOptCluster cluster, RelOptSchema relOptSchema) {
        super(context, cluster, relOptSchema);
    }

    public static MycatRelBuilder create(FrameworkConfig config) {
        return Frameworks.withPrepare(config,
                (cluster, relOptSchema, rootSchema, statement) ->
                        new MycatRelBuilder(config.getContext(), cluster, relOptSchema));
    }

    public  RelNode makeTransientSQLScan(String targetName, RelNode input,boolean forUpdate) {
        RelDataType rowType = input.getRowType();
        MycatConvention convention = getConvertion(targetName);
        return makeBySql(targetName,rowType,MycatCalciteSupport.INSTANCE.convertToSql(input, convention.dialect,forUpdate));
    }

    @NotNull
    private MycatConvention getConvertion(String targetName) {
        return MycatConvention.of(targetName,  new MycatSqlDialect(MycatSqlDialect.DEFAULT_CONTEXT){
            @Override
            public SqlNode getCastSpec(RelDataType type) {
                return super.getCastSpec(type);
            }

            @Override
            public String quoteIdentifier(String val) {
                return super.quoteIdentifier(val);
            }

            @Override
            public StringBuilder quoteIdentifier(StringBuilder buf, String val) {
                return super.quoteIdentifier(buf, val);
            }

            @Override
            public StringBuilder quoteIdentifier(StringBuilder buf, List<String> identifiers) {
                return super.quoteIdentifier(buf, identifiers);
            }

            @Override
            public void quoteStringLiteral(StringBuilder buf, String charsetName, String val) {
                buf.append(literalQuoteString);
                buf.append(val);
                buf.append(literalEndQuoteString);
            }

            @Override
            public void quoteStringLiteralUnicode(StringBuilder buf, String val) {
                super.quoteStringLiteralUnicode(buf, val);
            }

        });
    }


    /**
     * Creates a literal (constant expression).
     */
    public static RexNode literal(RelDataType type, Object value, boolean allowCast) {
        final RexBuilder rexBuilder = MycatCalciteSupport.INSTANCE.RexBuilder;
        JavaTypeFactoryImpl typeFactory = MycatCalciteSupport.INSTANCE.TypeFactory;
        RexNode literal;
        if (value == null) {
            literal = rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.NULL));
        } else if (value instanceof Boolean) {
            literal = rexBuilder.makeLiteral((Boolean) value);
        } else if (value instanceof BigDecimal) {
            literal = rexBuilder.makeExactLiteral((BigDecimal) value);
        } else if (value instanceof Float || value instanceof Double) {
            literal = rexBuilder.makeApproxLiteral(BigDecimal.valueOf(((Number) value).doubleValue()));
        } else if (value instanceof Number) {
            literal = rexBuilder.makeExactLiteral(BigDecimal.valueOf(((Number) value).longValue()));
        } else if (value instanceof String) {
            literal = rexBuilder.makeLiteral((String) value);
        } else if (value instanceof Enum) {
            literal = rexBuilder.makeLiteral(value, typeFactory.createSqlType(SqlTypeName.SYMBOL), false);
        } else if (value instanceof byte[]) {
            literal = rexBuilder.makeBinaryLiteral(new ByteString((byte[]) value));
        } else if (value instanceof LocalDate) {
            LocalDate value1 = (LocalDate) value;
            DateString dateString = new DateString(value1.getYear(), value1.getMonthValue(), value1.getDayOfMonth());
            literal = rexBuilder.makeDateLiteral(dateString);
        } else if (value instanceof LocalTime) {
            LocalTime value1 = (LocalTime) value;
            TimeString timeString = new TimeString(value1.getHour(), value1.getMinute(), value1.getSecond());
            literal = rexBuilder.makeTimeLiteral(timeString, -1);
        } else if (value instanceof LocalDateTime) {
            LocalDateTime value1 = (LocalDateTime) value;
            TimestampString timeString = new TimestampString(value1.getYear(), value1.getMonthValue(), value1.getDayOfMonth(), value1.getHour(), value1.getMinute(), value1.getSecond());
            timeString = timeString.withNanos(value1.getNano());
            literal = rexBuilder.makeTimestampLiteral(timeString, -1);
        } else {
            throw new IllegalArgumentException("cannot convert " + value
                    + " (" + value.getClass() + ") to a constant");
        }
        if (allowCast) {
            return rexBuilder.makeCast(type, literal);
        } else {
            return literal;
        }
    }

    public RelBuilder values(RelDataType rowType, Object... columnValues) {
        int columnCount = rowType.getFieldCount();
        final ImmutableList.Builder<ImmutableList<RexLiteral>> listBuilder =
                ImmutableList.builder();
        final List<RexLiteral> valueList = new ArrayList<>();
        List<RelDataTypeField> fieldList = rowType.getFieldList();
        for (int i = 0; i < columnValues.length; i++) {
            RelDataTypeField relDataTypeField = fieldList.get(valueList.size());
            valueList.add((RexLiteral) literal(relDataTypeField.getType(), columnValues[i], false));
            if ((i + 1) % columnCount == 0) {
                listBuilder.add(ImmutableList.copyOf(valueList));
                valueList.clear();
            }
        }
        super.values(listBuilder.build(), rowType);
        return this;
    }

    public RexNode literal(Object value) {
        return literal(null, value, false);
    }


    /**
     * todo for update
     * @param targetName
     * @param relDataType
     * @param sql
     * @return
     */
    public RelNode makeBySql(String targetName,RelDataType relDataType, String sql) {
        MycatConvention convention = MycatConvention.of(targetName, MycatSqlDialect.DEFAULT);
        MycatSQLTableScan transientTable = new MycatSQLTableScan(convention,relDataType,sql);
        id++;
        RelOptTable relOptTable = RelOptTableImpl.create(
                this.getRelOptSchema(),
                relDataType,
                transientTable,
                ImmutableList.of(id +"$"+targetName, id+sql));//名称唯一
        return new MycatTransientSQLTableScan(this.getCluster(), convention, relOptTable, () -> sql);
    }
}