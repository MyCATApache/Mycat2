package io.mycat.mpp;

import com.alibaba.fastsql.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.fastsql.sql.ast.statement.SQLJoinTableSource;
import io.mycat.mpp.plan.Column;
import io.mycat.mpp.plan.QueryPlan;
import io.mycat.mpp.plan.TableProvider;
import io.mycat.mpp.runtime.Type;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;

import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Objects;


/**
 * 基于stack形态的builder模式,
 * rex标量表达式部分不保存上下文
 * rel关系表达式部分使用stack构建图
 */
public class MyRelBuilder {
    private TableProvider tableProvider = new TableProvider();
    private final Deque<Frame> stack = new ArrayDeque<>();

    public SqlValue add(SqlValue left, SqlValue right) {
        return buildArithmeticBinaryOperator(SQLBinaryOperator.Add, left, right);
    }
    public SqlValue mod(SqlValue left, SqlValue right) {
        return null;
    }

    public SqlValue notGreaterThan(SqlValue left, SqlValue right) {
        return null;
    }

    public SqlValue isNot(SqlValue left, SqlValue right) {
        return null;
    }

    public SqlValue notLessThan(SqlValue left, SqlValue right) {
        return null;
    }

    public SqlValue notEqual(SqlValue left, SqlValue right) {
        return null;
    }

    public SqlValue bitwiseXorEQ(SqlValue leftExpr, SqlValue rightExpr) {
        return null;
    }

    public SqlValue multiply(SqlValue leftExpr, SqlValue rightExpr) {
        return null;
    }

    public SqlValue divide(SqlValue leftExpr, SqlValue rightExpr) {
        return null;
    }

    public SqlValue div(SqlValue leftExpr, SqlValue rightExpr) {
        return null;
    }

    public SqlValue subtract(SqlValue leftExpr, SqlValue rightExpr) {
        return null;
    }

    public SqlValue leftShift(SqlValue leftExpr, SqlValue rightExpr) {
        return null;
    }

    public SqlValue rightShift(SqlValue leftExpr, SqlValue rightExpr) {
        return null;
    }

    public SqlValue bitwiseOr(SqlValue leftExpr, SqlValue rightExpr) {
        return null;
    }

    public SqlValue bitwiseAnd(SqlValue leftExpr, SqlValue rightExpr) {
        return null;
    }

    public SqlValue greaterThan(SqlValue leftExpr, SqlValue rightExpr) {
        return null;
    }

    public SqlValue greaterThanOrEqual(SqlValue leftExpr, SqlValue rightExpr) {
        return null;
    }

    public SqlValue is(SqlValue leftExpr, SqlValue rightExpr) {
        return null;
    }

    public SqlValue lessThan(SqlValue leftExpr, SqlValue rightExpr) {
        return null;
    }

    public SqlValue lessThanOrEqual(SqlValue leftExpr, SqlValue rightExpr) {
        return null;
    }

    public SqlValue lessThanOrEqualOrGreaterThan(SqlValue leftExpr, SqlValue rightExpr) {
        return null;
    }

    public SqlValue lessThanOrGreater(SqlValue leftExpr, SqlValue rightExpr) {
        return null;
    }

    public SqlValue like(SqlValue leftExpr, SqlValue rightExpr) {
        return null;
    }

    public SqlValue notLike(SqlValue leftExpr, SqlValue rightExpr) {
        return null;
    }

    public SqlValue escape(SqlValue leftExpr, SqlValue rightExpr) {
        return null;
    }

    public SqlValue regExp(SqlValue leftExpr, SqlValue rightExpr) {
        return null;
    }

    public SqlValue notRegExp(SqlValue leftExpr, SqlValue rightExpr) {
        return null;
    }


    public SqlValue bitwiseNot(SqlValue leftExpr, SqlValue rightExpr) {
        return null;
    }

    public SqlValue concat(SqlValue leftExpr, SqlValue rightExpr) {
        return null;
    }

    public SqlValue booleanAnd(SqlValue leftExpr, SqlValue rightExpr) {
        return null;
    }

    public SqlValue booleanXor(SqlValue leftExpr, SqlValue rightExpr) {
        return null;
    }

    public SqlValue booleanOr(SqlValue leftExpr, SqlValue rightExpr) {
        return null;
    }

    public SqlValue bitwiseXor(SqlValue leftExpr, SqlValue rightExpr) {
        return null;
    }

    public SqlValue between(SqlValue leftExpr, SqlValue rightExpr) {
        return null;
    }

    public SqlValue literal(String text, String charset, String collate, String type) {

        return null;
    }

    public SqlValue literal(boolean booleanValue) {
        return null;
    }

    public SqlValue literal(byte[] bytes) {
        return null;
    }

    public SqlValue literal(String s) {
        return null;
    }

    public SqlValue literal(BigInteger value) {
        return null;
    }

    public SqlValue negative(SqlValue result) {
        return null;
    }

    public SqlValue not(SqlValue result) {
        return null;
    }

    public SqlValue bitInversion(SqlValue result) {
        return null;
    }

    public SqlValue cast2Binary(SqlValue result) {
        return null;
    }

    public SqlValue allColumn() {
        return null;
    }

    public SqlValue field(String name) {
        Column[] fields = Objects.requireNonNull(stack.peek()).getFields();
        for (int i = 0; i < fields.length; i++) {
            Column field = fields[i];
            if (name.equals(field.getName())) {
                return new AccessDataExpr(i, field.getType());
            }
        }
        return null;
    }

    public SqlValue nullLiteral() {
        return null;
    }

    public SqlValue literal(int number) {
        return IntExpr.of(number);
    }

    public SqlValue literal(Number number) {
        return null;
    }

    public SqlValue aggCall(String funcName, List<SqlValue> convertExprs, boolean isDistinct) {

        return null;
    }

    public SqlValue call(String methodName, List<SqlValue> convertExprs) {
        return null;
    }

    public SqlValue field(String tableName, String columnName) {
        return null;
    }

    public SqlValue field(String databaseName, String tableName, String columnName) {
        return null;
    }

    public SqlValue intervalLiteral(Object value1, String unit) {
        return null;
    }

    public SqlValue inList(SqlValue id, List<SqlValue> sqlExprs, boolean not) {
        return null;
    }

    public SqlValue cast(SqlValue leftExpr, Type type) {
        if (leftExpr.getType().equals(type)) return leftExpr;
        return new CastExpr(leftExpr, type);
    }

    public SqlValue cast(SqlValue leftExpr, Type type, Integer prec, Integer scale, boolean auto) {
        return null;
    }

    public SqlValue placeHolder(String name) {
        return null;
    }

    public MyRelBuilder push(QueryPlan sqlExpr) {
        stack.push(Frame.of(sqlExpr, sqlExpr.getType().getColumns()));
        return this;
    }


    public String getDefaultSchema() {
        return null;
    }

    public SqlValue alias(SqlValue expr, String alias) {
        return null;
    }

    public MyRelBuilder union(boolean all, int size) {
        return null;
    }

    public MyRelBuilder except(boolean all, int size) {
        return null;
    }

    public MyRelBuilder intersect(boolean all, int size) {
        return null;
    }

    public MyRelBuilder distinct(int size) {
        return null;
    }

    public SqlSource build() {
        return null;
    }

    public MyRelBuilder rename(List<String> strings) {
        return null;
    }

    public MyRelBuilder join(SQLJoinTableSource.JoinType joinType, SqlValue condition, List<SqlValue> using, boolean natural) {
        return null;
    }

    /**
     * 表别名
     *
     * @param alias2
     */
    public void alias(String alias2) {

    }

    public MyRelBuilder scan(String schemaName, String tableName) {
        QueryPlan queryPlan = tableProvider.create(schemaName, tableName);
        this.stack.push(Frame.of(queryPlan, queryPlan.getType().getColumns()));
        return this;
    }

    public SqlValue equality(SqlValue leftExpr, SqlValue rightExpr) {
        Type type = TypeSystem.resloveType(leftExpr.getType(), rightExpr.getType());
        leftExpr = cast(leftExpr, type);
        rightExpr = cast(rightExpr, type);
        return EqualityBinaryOp.create(leftExpr, rightExpr);
    }

//    public SqlValue makeCase(List<Case.Item> items, SqlValue value, SqlValue anElse) {
//        return null;
//    }

    @AllArgsConstructor
    @Getter
    private static class Frame {
        private final QueryPlan rel;
        private final Column[] fields;

        public static Frame of(QueryPlan queryPlan, Column[] fields) {
            return new Frame(queryPlan, fields);
        }
    }

    @NotNull
    public SqlValue buildArithmeticBinaryOperator(SQLBinaryOperator operator, SqlValue left, SqlValue right) {
        Type returnType = TypeSystem.resolveArithmeticType(left.getType(), right.getType());
        SqlValue leftExpr = cast(left, returnType);
        SqlValue rightExpr = cast(right, returnType);
        switch (operator){
            case Add:
               return AddNode.create(leftExpr,rightExpr,returnType);
            default:
                throw new UnsupportedOperationException();
        }
    }

}
