package io.mycat.mushroom;

import org.apache.calcite.rex.*;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SQLExpressionCompiler {
    final CompiledSQLExpressionFactory factory;

    public SQLExpressionCompiler(CompiledSQLExpressionFactory factory) {
        this.factory = factory;
    }

    public  CompiledSQLExpression compileExpression(RexNode expression) {
        if (expression == null) {
            return null;
        }
        switch (expression.getKind()) {
            case DYNAMIC_PARAM: {
                RexDynamicParam expression1 = (RexDynamicParam) expression;
                return factory.createRexDynamicParam(expression1.getIndex(), expression1.getType());
            }
            case LITERAL: {
                RexLiteral p = (RexLiteral) expression;
                if (p.isNull()) {
                    return factory.createConstantNullExpression();
                } else {
                    return factory.createConstantExpression(p.getValue3());
                }
            }
            case INPUT_REF: {
                RexInputRef p = (RexInputRef) expression;
                return factory.createAccessCurrentRowExpression(p.getIndex(), p.getType());
            }
            case FIELD_ACCESS: {
                RexFieldAccess p = (RexFieldAccess) expression;
                return factory.createAccessCurrentRowExpression(p.getField().getName()
                        , compileExpression(p.getReferenceExpr()));
            }
            case CORREL_VARIABLE: {
                RexCorrelVariable p = (RexCorrelVariable) expression;
                return factory.createRexCorrelVariable(p.id.getId(), p.id.getName());
            }
            case EQUALS: {
                RexCall p = (RexCall) expression;
                return factory.createEqualsExpression(
                        compileExpression(p.getOperands().get(0)),
                        compileExpression(p.getOperands().get(1)));
            }
            case NOT_EQUALS: {
                RexCall p = (RexCall) expression;
                return factory.createNotEqualsExpression(
                        compileExpression(p.getOperands().get(0)),
                        compileExpression(p.getOperands().get(1)));
            }
            case GREATER_THAN: {
                RexCall p = (RexCall) expression;
                return factory.createGreatThanExpression(
                        compileExpression(p.getOperands().get(0)),
                        compileExpression(p.getOperands().get(1)));
            }
            case GREATER_THAN_OR_EQUAL: {
                RexCall p = (RexCall) expression;
                return factory.createGreatThanOrEqualExpression(
                        compileExpression(p.getOperands().get(0)),
                        compileExpression(p.getOperands().get(1)));
            }
            case LESS_THAN: {
                RexCall p = (RexCall) expression;
                return factory.createLessThanExpression(
                        compileExpression(p.getOperands().get(0)),
                        compileExpression(p.getOperands().get(1)));
            }
            case LESS_THAN_OR_EQUAL: {
                RexCall p = (RexCall) expression;
                return factory.createLessThanOrEqualExpression(
                        compileExpression(p.getOperands().get(0)),
                        compileExpression(p.getOperands().get(1)));
            }
            case PLUS: {
                RexCall p = (RexCall) expression;
                return factory.createPlusExpression(
                        compileExpression(p.getOperands().get(0)),
                        compileExpression(p.getOperands().get(1)));
            }
            case MOD: {
                RexCall p = (RexCall) expression;
                return factory.createModExpression(
                        compileExpression(p.getOperands().get(0)),
                        compileExpression(p.getOperands().get(1)));
            }
            case MINUS: {
                RexCall p = (RexCall) expression;
                if (p.getOperands().size() == 2) {
                    return factory.createMinusExpression(
                            compileExpression(p.getOperands().get(0)),
                            compileExpression(p.getOperands().get(1)));
                }
                if (p.getOperands().size() == 1) {
                    return factory.createSignedExpression(
                            compileExpression(p.getOperands().get(0)));
                }
                throw new UnsupportedOperationException();
            }
            case TIMES: {
                RexCall p = (RexCall) expression;
                return factory.createMultiplyExpression(
                        compileExpression(p.getOperands().get(0)),
                        compileExpression(p.getOperands().get(1)));
            }
            case DIVIDE: {
                RexCall p = (RexCall) expression;
                return factory.createDivideExpression(
                        compileExpression(p.getOperands().get(0)),
                        compileExpression(p.getOperands().get(1)));
            }
            case LIKE: {
                RexCall p = (RexCall) expression;
                if (p.getOperands().size() == 2) {
                    return factory.createLikeExpression(
                            compileExpression(p.getOperands().get(0)), compileExpression(p.getOperands().get(0)),
                            compileExpression(p.getOperands().get(1)));
                } else {
                    return factory.createLikeExpression(
                            compileExpression(p.getOperands().get(0)),
                            compileExpression(p.getOperands().get(1)),
                            compileExpression(p.getOperands().get(2)));
                }
            }
            case AND: {
                RexCall p = (RexCall) expression;
                return factory.createAndExpression(
                        compileExpression(p.getOperands().get(0)),
                        compileExpression(p.getOperands().get(1)));
            }
            case OR: {
                RexCall p = (RexCall) expression;
                return factory.createOrExpression(
                        compileExpression(p.getOperands().get(0)),
                        compileExpression(p.getOperands().get(1)));
            }
            case NOT: {
                RexCall p = (RexCall) expression;
                return factory.createNotExpression(
                        compileExpression(p.getOperands().get(0)));
            }
            case IS_NOT_NULL: {
                RexCall p = (RexCall) expression;
                return factory.createIsNotNullExpression(
                        compileExpression(p.getOperands().get(0)));
            }
            case IS_NOT_TRUE: {
                RexCall p = (RexCall) expression;
                return factory.createIsNotTrueExpression(
                        compileExpression(p.getOperands().get(0)));
            }
            case IS_NULL: {
                RexCall p = (RexCall) expression;
                return factory.createIsNullExpression(
                        compileExpression(p.getOperands().get(0)));
            }
            case CAST: {
                RexCall p = (RexCall) expression;
                return factory.createCastExpression(compileExpression(p.getOperands().get(0)),p.getType());
            }
            case CASE: {
                RexCall p = (RexCall) expression;
                CompiledSQLExpression[] operands = p.getOperands().stream()
                        .map(i -> compileExpression(i)).toArray(n -> new CompiledSQLExpression[n]);
                List<Map.Entry<CompiledSQLExpression, CompiledSQLExpression>> cases = new ArrayList<>();
                boolean hasElse = operands.length % 2 == 1;
                int numcases = hasElse ? ((operands.length - 1) / 2) : (operands.length / 2);
                for (int j = 0; j < numcases; j++) {
                    cases.add(new AbstractMap.SimpleImmutableEntry<>(operands[j * 2], operands[j * 2 + 1]));
                }
                CompiledSQLExpression elseExp = hasElse ? operands[operands.length - 1] : null;
                return factory.createCaseExpression(cases, elseExp);
            }
            default:
                throw new IllegalStateException("Unexpected value: " + expression.getKind());
        }

    }
}
