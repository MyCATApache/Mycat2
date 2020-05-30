package io.mycat.mpp;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLDataType;
import com.alibaba.fastsql.sql.ast.SQLDataTypeImpl;
import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.SQLObject;
import com.alibaba.fastsql.sql.ast.expr.*;
import com.alibaba.fastsql.sql.ast.statement.SQLSelect;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectQuery;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.expr.MySqlCharExpr;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import io.mycat.mpp.runtime.Type;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class RexTranslator extends MySqlASTVisitorAdapter {
    SqlValue result = null;

    final MyRelBuilder relBuilder;

    public RexTranslator(MyRelBuilder rexBuilder) {
        this.relBuilder = rexBuilder;
    }

    SqlValue convertExpr(SQLExpr expr) {
        RexTranslator rexTranslator = new RexTranslator(relBuilder);
        expr.accept(rexTranslator);
        return rexTranslator.result;
    }

    final List<SqlValue> convertExprs(List<SQLExpr> exprs) {
        return exprs.stream().map(this::convertExpr).collect(Collectors.toList());
    }

    @Override
    public void endVisit(MySqlCharExpr x) {
        String charset = x.getCharset();
        String collate = x.getCollate();
        String type = x.getType();
        String text = x.getText();
        result = relBuilder.literal(text, charset, collate, type);
    }

    @Override
    public void endVisit(SQLQueryExpr x) {
        result = convertScalarSubQuery(x.getSubQuery().getQuery());
    }

    @Override
    public void endVisit(SQLBetweenExpr x) {
        result = relBuilder.between(convertExpr(x.getTestExpr()),convertExpr(x.getBeginExpr()), convertExpr(x.getEndExpr()));
    }

    @Override
    public void endVisit(SQLInSubQueryExpr x) {
        result = convertInSubQuery(convertExpr(x.getExpr()), x.isNot(), x.getSubQuery().getQuery());
    }

    @Override
    public void endVisit(SQLBooleanExpr x) {
        result = this.relBuilder.literal(x.getBooleanValue());
    }

    @Override
    public void endVisit(SQLBinaryOpExpr x) {
        switch (x.getOperator()) {
            case BitwiseXor:
                result = relBuilder.bitwiseXor(convertExpr(x.getLeft()), convertExpr(x.getRight()));
                break;
            case BitwiseXorEQ:
                result = relBuilder.bitwiseXorEQ(convertExpr(x.getLeft()), convertExpr(x.getRight()));
                break;
            case Multiply:
                result = relBuilder.multiply(convertExpr(x.getLeft()), convertExpr(x.getRight()));
                break;
            case Divide:
                result = relBuilder.divide(convertExpr(x.getLeft()), convertExpr(x.getRight()));
                break;
            case DIV:
                result = relBuilder.div(convertExpr(x.getLeft()), convertExpr(x.getRight()));
                break;
            case Modulus:
            case Mod:
                result = relBuilder.mod(convertExpr(x.getLeft()), convertExpr(x.getRight()));
                break;
            case Add:
                result = relBuilder.add(convertExpr(x.getLeft()), convertExpr(x.getRight()));
                break;
            case Subtract:
                result = relBuilder.subtract(convertExpr(x.getLeft()), convertExpr(x.getRight()));
                break;
            case LeftShift:
                result = relBuilder.leftShift(convertExpr(x.getLeft()), convertExpr(x.getRight()));
                break;
            case RightShift:
                result = relBuilder.rightShift(convertExpr(x.getLeft()), convertExpr(x.getRight()));
                break;
            case BitwiseAnd:
                result = relBuilder.bitwiseAnd(convertExpr(x.getLeft()), convertExpr(x.getRight()));
                break;
            case BitwiseOr:
                result = relBuilder.bitwiseOr(convertExpr(x.getLeft()), convertExpr(x.getRight()));
                break;
            case GreaterThan:
                result = relBuilder.greaterThan(convertExpr(x.getLeft()), convertExpr(x.getRight()));
                break;
            case GreaterThanOrEqual:
                result = relBuilder.greaterThanOrEqual(convertExpr(x.getLeft()), convertExpr(x.getRight()));
                break;
            case Is:
                result = relBuilder.is(convertExpr(x.getLeft()), convertExpr(x.getRight()));
                break;
            case LessThan:
                result = relBuilder.lessThan(convertExpr(x.getLeft()), convertExpr(x.getRight()));
                break;
            case LessThanOrEqual:
                result = relBuilder.lessThanOrEqual(convertExpr(x.getLeft()), convertExpr(x.getRight()));
                break;
            case LessThanOrEqualOrGreaterThan:
                result = relBuilder.lessThanOrEqualOrGreaterThan(convertExpr(x.getLeft()), convertExpr(x.getRight()));
                break;
            case LessThanOrGreater:
                result = relBuilder.lessThanOrGreater(convertExpr(x.getLeft()), convertExpr(x.getRight()));
                break;
            case Like:
                result = relBuilder.like(convertExpr(x.getLeft()), convertExpr(x.getRight()));
                break;
            case NotLike:
                result = relBuilder.notLike(convertExpr(x.getLeft()), convertExpr(x.getRight()));
                break;
            case NotEqual:
                result = relBuilder.notEqual(convertExpr(x.getLeft()), convertExpr(x.getRight()));
                break;
            case NotLessThan:
                result = relBuilder.notLessThan(convertExpr(x.getLeft()), convertExpr(x.getRight()));
                break;
            case NotGreaterThan:
                result = relBuilder.notGreaterThan(convertExpr(x.getLeft()), convertExpr(x.getRight()));
                break;
            case IsNot:
                result = relBuilder.isNot(convertExpr(x.getLeft()), convertExpr(x.getRight()));
                break;
            case Escape:
                throw new IllegalArgumentException();
//                result = relBuilder.escape(convertExpr(x.getLeft()), convertExpr(x.getRight()));
//                break;
            case RegExp:
                result = relBuilder.regExp(convertExpr(x.getLeft()), convertExpr(x.getRight()));
                break;
            case NotRegExp:
                result = relBuilder.notRegExp(convertExpr(x.getLeft()), convertExpr(x.getRight()));
                break;
            case Equality:
                result = relBuilder.equality(convertExpr(x.getLeft()), convertExpr(x.getRight()));
                break;
            case BitwiseNot:
                result = relBuilder.bitwiseNot(convertExpr(x.getLeft()), convertExpr(x.getRight()));
                break;
            case Concat:
                result = relBuilder.concat(convertExpr(x.getLeft()), convertExpr(x.getRight()));
                break;
            case PG_And:
            case BooleanAnd:
                result = relBuilder.booleanAnd(convertExpr(x.getLeft()), convertExpr(x.getRight()));
                break;
            case BooleanXor:
                result = relBuilder.booleanXor(convertExpr(x.getLeft()), convertExpr(x.getRight()));
                break;
            case BooleanOr:
                result = relBuilder.booleanOr(convertExpr(x.getLeft()), convertExpr(x.getRight()));
                break;
            case Assignment:
            case PG_ST_DISTANCE:
            case IsDistinctFrom:
            case IsNotDistinctFrom:
            case SoudsLike:
            case SubGt:
            case SubGtGt:
            case PoundGt:
            case PoundGtGt:
            case QuesQues:
            case QuesBar:
            case QuesAmp:
            case Union:
            case COLLATE:
            case ILike:
            case NotILike:
            case AT_AT:
            case SIMILAR_TO:
            case POSIX_Regular_Match:
            case POSIX_Regular_Match_Insensitive:
            case POSIX_Regular_Not_Match:
            case POSIX_Regular_Not_Match_POSIX_Regular_Match_Insensitive:
            case Array_Contains:
            case Array_ContainedBy:
            case SAME_AS:
            case JSONContains:
            case RLike:
            case NotRLike:
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    public void endVisit(SQLUnaryExpr x) {
        this.result = convertExpr(x.getExpr());

        switch (x.getOperator()) {
            case Negative:
                result = relBuilder.negative(result);
                break;
            case NOT:
            case Not:
                result = relBuilder.not(result);
                break;
            case Compl:
                result = relBuilder.bitInversion(result);
                break;
            case BINARY:
                result = relBuilder.cast2Binary(result);
                break;
            case Plus:
                this.result = this.result;
                break;
            case Prior:
            case ConnectByRoot:
            case RAW:
            case Pound:
            default:
                throw new UnsupportedOperationException();
        }

    }

    @Override
    public void endVisit(SQLInListExpr x) {
        boolean not = x.isNot();
        SqlValue id = convertExpr(x.getExpr());
        List<SqlValue> sqlExprs = convertExprs(x.getTargetList());
        result = relBuilder.inList(id, sqlExprs, not);
    }


    @Override
    public void endVisit(SQLIntervalExpr x) {
        SQLExpr value = x.getValue();
        if (value instanceof SQLValuableExpr) {
            Object value1 = ((SQLValuableExpr) value).getValue();
            String unit = x.getUnit().name();
            result = relBuilder.intervalLiteral(value1, unit);
            return;
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public void endVisit(SQLNotExpr x) {
        result = relBuilder.not(convertExpr(x.getExpr()));
    }

    @Override
    public void endVisit(SQLAllColumnExpr x) {
        result = relBuilder.allColumn();
    }

    @Override
    public void endVisit(SQLCaseExpr x) {
//        Case a = new Case();
//        x.getItems().forEach(item -> a.addItem(convertExpr(item.getConditionExpr()), convertExpr(item.getValueExpr())));
//        Optional.ofNullable(x.getValueExpr()).ifPresent(c -> a.setValue(convertExpr(c)));
//        Optional.ofNullable(x.getElseExpr()).ifPresent(c -> a.setElse(convertExpr(c)));
//        result = relBuilder.makeCase(a.getItems(),a.getValue(),a.getElse());
    }

    @Override
    public void endVisit(SQLCastExpr x) {
        boolean auto = x.isTry();
        SQLExpr expr = x.getExpr();
        SQLDataType dataType = x.computeDataType();
        Integer prec = getPrec(dataType);
        Integer scale = getScale(dataType);
        String type = dataType.getName();
        result = relBuilder.cast(convertExpr(expr), Type.of(type,true).getBase(), prec, scale, auto);
    }

    private Integer getScale(SQLDataType dataType) {
        SQLDataTypeImpl dataType1 = (SQLDataTypeImpl) (dataType);
        return Optional.ofNullable(dataType1.getArguments())
                .filter(i -> i.size() > 1)
                .map(i -> i.get(1))
                .map(i -> (SQLNumericLiteralExpr) i)
                .map(i -> i.getNumber())
                .map(i -> i.intValue()).orElse(null);
    }

    private Integer getPrec(SQLDataType dataType) {
        SQLDataTypeImpl dataType1 = (SQLDataTypeImpl) (dataType);
        return Optional.ofNullable(dataType1.getArguments())
                .filter(i -> i.size() > 0)
                .map(i -> i.get(0))
                .map(i -> (SQLNumericLiteralExpr) i)
                .map(i -> i.getNumber())
                .map(i -> i.intValue()).orElse(null);
    }

    @Override
    public void endVisit(SQLCharExpr x) {
        result = relBuilder.literal(x.getText());
    }

    @Override
    public void endVisit(SQLIdentifierExpr x) {
        result = relBuilder.field(x.getName());
    }

    @Override
    public void endVisit(SQLNullExpr x) {
        result = relBuilder.nullLiteral();
    }

    @Override
    public void endVisit(SQLIntegerExpr x) {
        result = relBuilder.literal(x.getNumber().longValue());
    }

    @Override
    public void endVisit(SQLNCharExpr x) {
        result = relBuilder.literal(x.getText());
    }

    @Override
    public void endVisit(SQLNumberExpr x) {
        result = relBuilder.literal(x.getNumber());
    }

    @Override
    public void endVisit(SQLVariantRefExpr x) {
        result = relBuilder.placeHolder(x.getName());
    }

    @Override
    public void endVisit(SQLPropertyExpr x) {
        String columnName = x.getName();
        if (x.getOwner() == null) {
            result = relBuilder.field(columnName);
            return;
        }
        if (x.getOwner() instanceof SQLIdentifierExpr) {
            result = relBuilder.field(((SQLIdentifierExpr) x.getOwner()).normalizedName(), columnName);
            return;
        }
        if (x.getOwner() instanceof SQLPropertyExpr) {
            SQLPropertyExpr owner = (SQLPropertyExpr) x.getOwner();
            if (owner.getOwner() instanceof SQLIdentifierExpr) {
                String databaseName = SQLUtils.normalize(((SQLIdentifierExpr) owner.getOwner()).getName());
                String tableName = SQLUtils.normalize(owner.getName());
                result = relBuilder.field(databaseName, tableName, columnName);
                return;
            }
        }
        if (x.getOwner() instanceof SQLVariantRefExpr) {
            result = relBuilder.placeHolder(x.normalizedName());
            return;
        }
        //SQLVariantRefExpr
    }


    @Override
    public void endVisit(SQLAggregateExpr x) {
        boolean isDistinct = false;
        switch (Optional.ofNullable(x.getOption()).orElse(SQLAggregateOption.ALL)) {
            case DISTINCT:
                isDistinct = true;
                break;
            case ALL:
                break;
            case UNIQUE:
            case DEDUPLICATION:
            default:
                throw new UnsupportedOperationException();
        }
        result = relBuilder.aggCall(x.getMethodName(), convertExprs(x.getArguments()), isDistinct);
    }

    @Override
    public void endVisit(SQLMethodInvokeExpr x) {
        result = relBuilder.call(x.getMethodName(), convertExprs(x.getArguments()));
    }

    @Override
    public void endVisit(SQLListExpr x) {
//        List<SqlAbs> sqlExprs = convertExprs(x.getItems());
//        rexBuilder.values(sqlExprs);
//        result = rexBuilder.pop();
        //不应该在这里处理,SQLValuesTableSource
        throw new UnsupportedOperationException();
    }

    @Override
    public void endVisit(SQLAllExpr x) {
        rightSubQuery(true, x.getParent(), x.getSubQuery());
    }

    private void rightSubQuery(boolean all, SQLObject parent2, SQLSelect subQuery) {
        if (parent2 instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr parent = (SQLBinaryOpExpr) parent2;
            if (simplyBinarySubQuery(parent ,all)) {
                SqlValue sqlAbs = convertExpr(parent.getLeft());
                result = convertAnySubQuery(sqlAbs, parent.getOperator(), subQuery.getQuery());
                return;
            } else {
                boolean not = false;
                switch (parent.getOperator()) {
                    case NotEqual:
                    case LessThanOrGreater:
                        not = true;
                        break;
                    default:
                }
                result = convertInSubQuery(convertExpr(parent.getLeft()), not, subQuery.getQuery());
                return;
            }
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public void endVisit(SQLSomeExpr x) {
        rightSubQuery(false, x.getParent(), x.getSubQuery());
    }


    @Override
    public void endVisit(SQLAnyExpr x) {
        rightSubQuery(false, x.getParent(), x.getSubQuery());
    }

    private static boolean simplyBinarySubQuery(SQLBinaryOpExpr parent, boolean all) {
        boolean allAnySubQuery;
        switch (parent.getOperator()) {
            case Equality:
                allAnySubQuery = all;
                break;
            case NotEqual:
            case LessThanOrGreater:
                allAnySubQuery = !all;
                break;
            case LessThan:
            case LessThanOrEqual:
            case GreaterThan:
            case GreaterThanOrEqual:
                allAnySubQuery = true;
                break;
            default:
                throw new UnsupportedOperationException();
        }
        return allAnySubQuery;
    }

    private SqlValue convertAnySubQuery(SqlValue left, SQLBinaryOperator operator, SQLSelectQuery query) {
//        SqlAbsSource sqlExpr = convertQuery(query);
//        return new AnySubQuery(left,operator,sqlExpr);
        return null;
    }


    public SqlValue convertScalarSubQuery(SQLSelectQuery query) {
//        SqlAbsSource sqlAbs = convertQuery(query);
//        return new ScalarSubQuery(sqlAbs);
        return null;
    }

    public SqlValue convertInSubQuery(SqlValue expr, boolean not, SQLSelectQuery query) {
//        SqlAbsSource sqlExpr = convertQuery(query);
//        return new InSubQuery(expr,not,sqlExpr);
        return null;
    }

    public SqlValue existsSubQuery(boolean not, SQLSelectQuery query) {
//        SqlAbsSource sqlExpr = convertQuery(query);
//        return new ExistsSubQuery(not,sqlExpr);
        return null;
    }

    private SqlValue convertQuery(SQLSelectQuery query) {
//        SqlToExprTranslator sqlToExprTranslator = new SqlToExprTranslator(relBuilder);
//        return sqlToExprTranslator.convertQueryRecursive(query);
        return null;
    }

    @Override
    public void endVisit(SQLExistsExpr x) {
        result = existsSubQuery(x.isNot(), x.getSubQuery().getQuery());
    }

    @Override
    public void endVisit(SQLBinaryExpr x) {
        String text = x.getText();
        if ("".equals(text)) {
            result = relBuilder.literal("");
        } else {
            result = relBuilder.literal((BigInteger) x.getValue());
        }
    }

    @Override
    public void endVisit(SQLHexExpr x) {
        result = relBuilder.hexLiteral(x.getHex());
    }

    @Override
    public void endVisit(SQLSelectStatement node) {
        result = convertScalarSubQuery(node.getSelect().getQuery());
    }

}