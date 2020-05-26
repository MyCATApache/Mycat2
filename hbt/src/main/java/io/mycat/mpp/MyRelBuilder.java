package io.mycat.mpp;

import com.alibaba.fastsql.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.fastsql.sql.ast.statement.SQLJoinTableSource;
import io.mycat.mpp.plan.Column;
import io.mycat.mpp.plan.QueryPlan;
import io.mycat.mpp.plan.TableProvider;
import io.mycat.mpp.runtime.Type;
import lombok.AllArgsConstructor;
import lombok.Getter;

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
    private final TableProvider tableProvider = new TableProvider();
    private final Deque<Frame> stack = new ArrayDeque<>();

    public SqlValue add(SqlValue leftExpr, SqlValue rightExpr) {
        return call(SQLBinaryOperator.Add, leftExpr, rightExpr);
    }

    public SqlValue mod(SqlValue leftExpr, SqlValue rightExpr) {
        return call(SQLBinaryOperator.Mod, leftExpr, rightExpr);
    }

    public SqlValue notGreaterThan(SqlValue leftExpr, SqlValue rightExpr) {
        return call(SQLBinaryOperator.NotGreaterThan, leftExpr, rightExpr);
    }

    public SqlValue isNot(SqlValue leftExpr, SqlValue rightExpr) {
        return call(SQLBinaryOperator.NotGreaterThan, leftExpr, rightExpr);
    }

    public SqlValue notLessThan(SqlValue leftExpr, SqlValue rightExpr) {
        return call(SQLBinaryOperator.NotLessThan, leftExpr, rightExpr);
    }

    public SqlValue notEqual(SqlValue leftExpr, SqlValue rightExpr) {
        return call(SQLBinaryOperator.NotEqual, leftExpr, rightExpr);
    }

    public SqlValue bitwiseXorEQ(SqlValue leftExpr, SqlValue rightExpr) {
        return call(SQLBinaryOperator.NotEqual, leftExpr, rightExpr);
    }

    public SqlValue multiply(SqlValue leftExpr, SqlValue rightExpr) {
        return call(SQLBinaryOperator.Multiply, leftExpr, rightExpr);
    }

    public SqlValue divide(SqlValue leftExpr, SqlValue rightExpr) {
        return call(SQLBinaryOperator.Divide, leftExpr, rightExpr);
    }

    public SqlValue div(SqlValue leftExpr, SqlValue rightExpr) {
        return call(SQLBinaryOperator.DIV, leftExpr, rightExpr);
    }

    public SqlValue subtract(SqlValue leftExpr, SqlValue rightExpr) {
        return call(SQLBinaryOperator.Subtract, leftExpr, rightExpr);
    }

    public SqlValue leftShift(SqlValue leftExpr, SqlValue rightExpr) {
        return call(SQLBinaryOperator.LeftShift, leftExpr, rightExpr);
    }

    public SqlValue rightShift(SqlValue leftExpr, SqlValue rightExpr) {
        return call(SQLBinaryOperator.RightShift, leftExpr, rightExpr);
    }

    public SqlValue bitwiseOr(SqlValue leftExpr, SqlValue rightExpr) {
        return call(SQLBinaryOperator.BitwiseOr, leftExpr, rightExpr);
    }

    public SqlValue bitwiseAnd(SqlValue leftExpr, SqlValue rightExpr) {
        return call(SQLBinaryOperator.BitwiseAnd, leftExpr, rightExpr);
    }

    public SqlValue greaterThan(SqlValue leftExpr, SqlValue rightExpr) {
        return call(SQLBinaryOperator.GreaterThan, leftExpr, rightExpr);
    }

    public SqlValue greaterThanOrEqual(SqlValue leftExpr, SqlValue rightExpr) {
        return call(SQLBinaryOperator.GreaterThanOrEqual, leftExpr, rightExpr);
    }

    public SqlValue is(SqlValue leftExpr, SqlValue rightExpr) {
        return call(SQLBinaryOperator.Is, leftExpr, rightExpr);
    }

    public SqlValue lessThan(SqlValue leftExpr, SqlValue rightExpr) {
        return call(SQLBinaryOperator.LessThan, leftExpr, rightExpr);
    }

    public SqlValue lessThanOrEqual(SqlValue leftExpr, SqlValue rightExpr) {
        return call(SQLBinaryOperator.LessThanOrEqual, leftExpr, rightExpr);
    }

    public SqlValue lessThanOrEqualOrGreaterThan(SqlValue leftExpr, SqlValue rightExpr) {
        return call(SQLBinaryOperator.LessThanOrEqualOrGreaterThan, leftExpr, rightExpr);
    }

    public SqlValue lessThanOrGreater(SqlValue leftExpr, SqlValue rightExpr) {
        return call(SQLBinaryOperator.LessThanOrEqualOrGreaterThan, leftExpr, rightExpr);
    }

    public SqlValue like(SqlValue leftExpr, SqlValue rightExpr) {
        return call(SQLBinaryOperator.Like, leftExpr, rightExpr);
    }

    public SqlValue notLike(SqlValue leftExpr, SqlValue rightExpr) {
        return call(SQLBinaryOperator.NotLike, leftExpr, rightExpr);
    }

    public SqlValue regExp(SqlValue leftExpr, SqlValue rightExpr) {
        return call(SQLBinaryOperator.RegExp, leftExpr, rightExpr);
    }

    public SqlValue notRegExp(SqlValue leftExpr, SqlValue rightExpr) {
        return call(SQLBinaryOperator.NotRegExp, leftExpr, rightExpr);
    }

    public SqlValue bitwiseNot(SqlValue leftExpr, SqlValue rightExpr) {
        return call(SQLBinaryOperator.BitwiseNot, leftExpr, rightExpr);
    }

    public SqlValue concat(SqlValue leftExpr, SqlValue rightExpr) {
        return call(SQLBinaryOperator.Concat, leftExpr, rightExpr);
    }

    public SqlValue booleanAnd(SqlValue leftExpr, SqlValue rightExpr) {
        return call(SQLBinaryOperator.BitwiseAnd, leftExpr, rightExpr);
    }

    public SqlValue booleanXor(SqlValue leftExpr, SqlValue rightExpr) {
        return call(SQLBinaryOperator.BooleanXor, leftExpr, rightExpr);
    }

    public SqlValue booleanOr(SqlValue leftExpr, SqlValue rightExpr) {
        return call(SQLBinaryOperator.BooleanOr, leftExpr, rightExpr);
    }

    public SqlValue bitwiseXor(SqlValue leftExpr, SqlValue rightExpr) {
        return call(SQLBinaryOperator.BooleanXor, leftExpr, rightExpr);
    }

    public SqlValue between(SqlValue expr, SqlValue leftExpr, SqlValue rightExpr) {
        Type returnType = TypeSystem.resloveType(leftExpr.getType(), rightExpr.getType());
        returnType = TypeSystem.resloveType(expr.getType(), returnType);
        leftExpr = cast(leftExpr, returnType);
        rightExpr = cast(rightExpr, returnType);
        expr = cast(expr, returnType);
        return BetweenNode.create(expr, leftExpr, rightExpr);
    }

    public SqlValue literal(String text, String charset, String collate, String type) {

        return null;
    }

    public SqlValue literal(boolean booleanValue) {
        return BooleanSqlValue.create(booleanValue);
    }

    public SqlValue literal(String s) {
        return StringSqlValue.create(s);
    }

    public SqlValue literal(BigInteger value) {
        return BigIntSqlValue.create(value);
    }

    public SqlValue negative(SqlValue result) {
        return null;
    }

    public SqlValue not(SqlValue result) {
        return NotNode.create(result);
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

    public SqlValue literal(long number) {
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
        return call(SQLBinaryOperator.Equality, leftExpr, rightExpr);
    }


    @AllArgsConstructor
    @Getter
    private static class Frame {
        private final QueryPlan rel;
        private final Column[] fields;

        public static Frame of(QueryPlan queryPlan, Column[] fields) {
            return new Frame(queryPlan, fields);
        }
    }


    public SqlValue call(SQLBinaryOperator operator, SqlValue left, SqlValue right) {
        Type returnType = operator.isArithmetic() ?
                TypeSystem.resolveArithmeticType(left.getType(), right.getType()) :
                TypeSystem.resloveType(left.getType(), right.getType());
        SqlValue leftExpr = cast(left, returnType);
        SqlValue rightExpr = cast(right, returnType);
        return buildBinaryOperator(operator, returnType, leftExpr, rightExpr);
    }

    public SqlValue buildBinaryOperator(SQLBinaryOperator operator, Type returnType, SqlValue leftExpr, SqlValue rightExpr) {
        switch (operator) {
            case Union:
            case COLLATE:
                throw new IllegalArgumentException();
            case BitwiseXor:
                return BitwiseXorNode.create(leftExpr, rightExpr, returnType);
            case BitwiseXorEQ:
                return BitwiseXorEQNode.create(leftExpr, rightExpr, returnType);
            case Multiply:
                return MultiplyNode.create(leftExpr, rightExpr, returnType);
            case Divide:
                return DivideNode.create(leftExpr, rightExpr, returnType);
            case DIV:
                return DivNode.create(leftExpr, rightExpr, returnType);
            case Modulus:
            case Mod:
                return ModNode.create(leftExpr, rightExpr, returnType);
            case Add:
                return AddNode.create(leftExpr, rightExpr, returnType);
            case Subtract:
                return SubtractNode.create(leftExpr, rightExpr, returnType);
            case SubGt:
                return SubGtNode.create(leftExpr, rightExpr);
            case SubGtGt:
                break;
            case PoundGt:
                break;
            case PoundGtGt:
                break;
            case QuesQues:
                break;
            case QuesBar:
                break;
            case QuesAmp:
                break;
            case LeftShift:
                return LeftShiftNode.create(leftExpr, rightExpr, returnType);
            case RightShift:
                return RightShiftNode.create(leftExpr, rightExpr, returnType);
            case BitwiseAnd:
                return BitwiseAndNode.create(leftExpr, rightExpr, returnType);
            case BitwiseOr:
                return BitwiseOrNode.create(leftExpr, rightExpr, returnType);
            case GreaterThan:
                return GreaterThanNode.create(leftExpr, rightExpr);
            case GreaterThanOrEqual:
                return GreaterThanOrEqualNode.create(leftExpr, rightExpr);
            case Is:
                return IsNode.create(leftExpr, rightExpr);
            case LessThan:
                return LessThanNode.create(leftExpr, rightExpr);
            case LessThanOrEqual:
                return LessThanOrEqualNode.create(leftExpr, rightExpr);
            case LessThanOrEqualOrGreaterThan:
                return LessThanOrEqualOrGreaterThanNode.create(leftExpr, rightExpr);
            case LessThanOrGreater:
                return LessThanOrGreaterNode.create(leftExpr, rightExpr);
            case IsDistinctFrom:
                break;
            case IsNotDistinctFrom:
                break;
            case Like:
                return LikeNode.create(leftExpr, rightExpr);
            case SoudsLike:
                break;
            case NotLike:
                return LessThanOrGreaterNode.create(leftExpr, rightExpr);
            case ILike:
                break;
            case NotILike:
                break;
            case AT_AT:
                break;
            case SIMILAR_TO:
                break;
            case POSIX_Regular_Match:
                break;
            case POSIX_Regular_Match_Insensitive:
                break;
            case POSIX_Regular_Not_Match:
                break;
            case POSIX_Regular_Not_Match_POSIX_Regular_Match_Insensitive:
                break;
            case Array_Contains:
                break;
            case Array_ContainedBy:
                break;
            case SAME_AS:
                break;
            case JSONContains:
                break;
            case RLike:
                break;
            case NotRLike:
                break;
            case NotEqual:
                return NotEqualNode.create(leftExpr, rightExpr);
            case NotLessThan:
                return NotLessThanNode.create(leftExpr, rightExpr);
            case NotGreaterThan:
                return NotGreaterThanNode.create(leftExpr, rightExpr);
            case IsNot:
                return IsNotNode.create(leftExpr, rightExpr);
            case RegExp:
                return RegExpNode.create(leftExpr, rightExpr);
            case NotRegExp:
                return NotRegExpNode.create(leftExpr, rightExpr);
            case Equality:
                return EqualityBinaryOp.create(leftExpr, rightExpr);
            case BitwiseNot:
                return BitwiseNotNode.create(leftExpr, rightExpr, returnType);
            case Concat:
                return ConcatNode.create(leftExpr, rightExpr, returnType);
            case PG_And:
            case BooleanAnd:
                return AndNode.create(leftExpr, rightExpr);
            case BooleanXor:
                return BooleanXorNode.create(leftExpr, rightExpr);
            case BooleanOr:
                return OrNode.create(leftExpr, rightExpr);
            case Escape:
            case Assignment:
            case PG_ST_DISTANCE:
            default:

        }

        throw new UnsupportedOperationException();
    }

}
