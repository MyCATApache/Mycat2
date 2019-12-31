package io.mycat.rsqlBuilder;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.Util;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

public class DesBuilder extends RelBuilder {
    private final RelFactories.ValuesFactory valuesFactory;
    private final RexBuilder rexBuilder;

    protected DesBuilder(Context context, RelOptCluster cluster, RelOptSchema relOptSchema) {
        super(context, cluster, relOptSchema);
        this.valuesFactory = Util.first(context.unwrap(RelFactories.ValuesFactory.class),
                RelFactories.DEFAULT_VALUES_FACTORY);
        this.rexBuilder = this.getRexBuilder();
    }

    public static DesBuilder create(FrameworkConfig config) {

        return Frameworks.withPrepare(config,
                (cluster, relOptSchema, rootSchema, statement) -> new DesBuilder(config.getContext(), cluster, relOptSchema));
    }

    public RelBuilder values2(RelDataType rowType, Object... columnValues) {
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
        push(this.valuesFactory.createValues(this.getCluster(), rowType, listBuilder.build()));
        return this;
    }

    public RexNode literal(Object value) {
        return literal(null, value, false);
    }

    /**
     * Creates a literal (constant expression).
     */
    public RexNode literal(RelDataType type, Object value, boolean allowCast) {
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        RexNode literal;
        if (value == null) {
            literal = rexBuilder.makeNullLiteral(getTypeFactory().createSqlType(SqlTypeName.NULL));
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
            literal = rexBuilder.makeLiteral(value, getTypeFactory().createSqlType(SqlTypeName.SYMBOL), false);
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
}