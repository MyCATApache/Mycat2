package io.mycat.hbt.ast;

import io.mycat.hbt.ast.base.*;
import io.mycat.hbt.ast.query.*;
import org.apache.calcite.sql.type.SqlTypeName;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.List;

public interface HBTBuiltinHelper {

    default public String Bool() {
        return SqlTypeName.BOOLEAN.getName();
    }

    default public String Tinyint() {
        return SqlTypeName.TINYINT.getName();
    }

    default public String Smallint() {
        return SqlTypeName.SMALLINT.getName();
    }

    default public String Integer() {
        return SqlTypeName.INTEGER.getName();
    }

    default public String Bigint() {
        return SqlTypeName.BIGINT.getName();
    }

    default public String Decimal() {
        return SqlTypeName.DECIMAL.getName();
    }

    default public String Float() {
        return SqlTypeName.FLOAT.getName();
    }

    default public String Real() {
        return SqlTypeName.REAL.getName();
    }

    default public String Double() {
        return SqlTypeName.DOUBLE.getName();
    }

    default public String Date() {
        return SqlTypeName.DATE.getName();
    }

    default public String Time() {
        return SqlTypeName.TIME.getName();
    }

    default public String TimeWithLocalTimeZone() {
        return SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE.getName();
    }

    default public String Timestamp() {
        return SqlTypeName.TIMESTAMP.getName();
    }

    default public String TimestampWithLocalTimeZone() {
        return SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE.getName();
    }

    default public String IntervalYear() {
        return SqlTypeName.INTERVAL_YEAR.getName();
    }

    default public String IntervalYearMonth() {
        return SqlTypeName.INTERVAL_YEAR_MONTH.getName();
    }

    default public String IntervalMonth() {
        return SqlTypeName.INTERVAL_MONTH.getName();
    }

    default public String IntervalDay() {
        return SqlTypeName.INTERVAL_DAY.getName();
    }

    default public String IntervalDayHour() {
        return SqlTypeName.INTERVAL_DAY_HOUR.getName();
    }

    default public String IntervalDayMinute() {
        return SqlTypeName.INTERVAL_DAY_MINUTE.getName();
    }

    default public String IntervalDaySecond() {
        return SqlTypeName.INTERVAL_DAY_SECOND.getName();
    }

    default public String IntervalHour() {
        return SqlTypeName.INTERVAL_HOUR.getName();
    }

    default public String IntervalHourMinute() {
        return SqlTypeName.INTERVAL_HOUR_MINUTE.getName();
    }

    default public String IntervalMinute() {
        return SqlTypeName.INTERVAL_MINUTE.getName();
    }

    default public String IntervalMinuteSecond() {
        return SqlTypeName.INTERVAL_MINUTE_SECOND.getName();
    }

    default public String IntervalSecond() {
        return SqlTypeName.INTERVAL_SECOND.getName();
    }

    default public String Char() {
        return SqlTypeName.CHAR.getName();
    }

    default public String Varchar() {
        return SqlTypeName.VARCHAR.getName();
    }

    default public String Binary() {
        return SqlTypeName.BINARY.getName();
    }

    default public String Varbinary() {
        return SqlTypeName.VARBINARY.getName();
    }

    default public String Null() {
        return SqlTypeName.NULL.getName();
    }

    default public FieldType fieldType(String columnName, HBTTypes columnType, boolean columnNullable) {
        return new FieldType(columnName, columnType.getName(), columnNullable, null, null);
    }

    default public FieldType fieldType(String columnName, String columnType, boolean columnNullable) {
        return new FieldType(columnName, columnType, columnNullable, null, null);
    }

    default public FieldType fieldType(String columnName, String columnType, boolean columnNullable, Integer precision, Integer scale) {
        return new FieldType(columnName, columnType, columnNullable, precision, scale);
    }

    default public Schema set(List<FieldType> fieldTypes, List<Object> values) {
        return new AnonyTableSchema(fieldTypes, values);
    }

    @NotNull
    default Schema filter(Schema schema, Expr expr) {
        return new FilterSchema(schema, expr);
    }

    @NotNull
    default Schema correlate(HBTOp op, String refName, Schema leftschema, Schema rightschema) {
        return new CorrelateSchema(op, refName, leftschema, rightschema);
    }

    @NotNull
    default Schema join(HBTOp op, Expr expr, Schema left, Schema right) {
        return new JoinSchema(op, expr, left, right);
    }

    @NotNull
    default Schema distinct(Schema schema) {
        return new DistinctSchema(schema);
    }

    @NotNull
    default Schema table(List<FieldType> fields, List<Object> values) {
        return new AnonyTableSchema(fields, values);
    }

    @NotNull
    default Schema groupBy(Schema schema, List<GroupKey> groupkeys, List<AggregateCall> aggregating) {
        return new GroupBySchema(schema, groupkeys, aggregating);
    }

    @NotNull
    default Schema orderBy(Schema schema, List<OrderItem> orderItemList) {
        return new OrderSchema(schema, orderItemList);
    }

    @NotNull
    default Schema limit(Schema schema, Number offset, Number limit) {
        return new LimitSchema(schema, offset, limit);
    }

    @NotNull
    default Schema map(Schema schema, List<Expr> collect) {
        return new MapSchema(schema, collect);
    }

    default Schema fromTable(String schema, String table) {
        return fromTable(Arrays.asList(schema, table));
    }

    default Schema fromTable(List<String> collect) {
        return new FromTableSchema(collect);
    }

    @NotNull
    default Schema set(HBTOp op, List<Schema> collect) {
        return new SetOpSchema(op, collect);
    }


    default OrderItem order(String identifier, Direction direction) {
        return new OrderItem(identifier, direction);
    }

}