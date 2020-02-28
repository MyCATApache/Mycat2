/**
 * Copyright (C) <2020>  <chen junwen>
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
package io.mycat.hbt.parser;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.parser.ParserException;
import com.alibaba.fastsql.sql.parser.Token;
import io.mycat.hbt.HBTCalciteSupport;
import io.mycat.hbt.parser.literal.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author jamie12221
 **/
public class HBTParser {
    private final Lexer lexer;
    private int paramCount  = 0;
    private static final Map<String, Precedence> operators = HBTCalciteSupport.INSTANCE.getOperators();

    public HBTParser(String text) {
        this.lexer = new Lexer(text);
        this.lexer.nextToken();
    }

    private String getOp() {
        String op = lexer.tokenString();
        Precedence precedence = operators.get(op);
        if (precedence != null) {
            op = precedence.opText;
        }
        return op;
    }


    public ParseNode statement() {
        ParseNode expression = expression();
        if (lexer.token() == Token.SEMI) {
            lexer.nextToken();
        } else if (!lexer.isEOF()) {
            throw new ParserException();
        }
        return expression;
    }

    public ParseNode expression() {
        ParseNode right = primary();
        Precedence next = null;
        while ((next = operators.get(lexer.tokenString())) != null) {
            right = doShift(right, next.value);
        }
        return right;
    }

    private ParseNode doShift(ParseNode left, int prec) {
        String op = getOp();
        lexer.nextToken();
        ParseNode right = primary();
        Precedence next = null;
        while ((next = operators.get(lexer.tokenString())) != null && rightIsExpr(prec, next)) {
            right = doShift(right, next.value);
        }
        return dotCall(new CallExpr(op, new ParenthesesExpr(left, right)));
    }

    private CallExpr dotCall(CallExpr call) {
        List<ParseNode> args = call.getArgs().getExprs();
        if (args.size() == 2 && "DOT".equalsIgnoreCase(call.getName()) && (args.get(1) instanceof CallExpr)) {
            ArrayList<ParseNode> objects = new ArrayList<>();
            objects.add(args.get(0));
            CallExpr callExpr = (CallExpr) args.get(1);
            objects.addAll(callExpr.getArgs().getExprs());
            return new CallExpr(callExpr.getName(), new ParenthesesExpr(objects));
        }
        return call;
    }

    public ParseNode primary() {
        Token token = lexer.token();
        switch (token) {
            default:
            case IDENTIFIER: {
                String id = lexer.tokenIdentifier();
                lexer.nextToken();
                if (lexer.token() == Token.LPAREN) {
                    return dotCall(new CallExpr(id, parentheresExpr()));
                }
                if (id.startsWith("`") && id.endsWith("`")) {
                    return new IdLiteral(SQLUtils.normalize(id));
                }
                if ("true".equals(id)) {
                    return new BooleanLiteral(true);
                }
                if ("false".equals(id)) {
                    return new BooleanLiteral(false);
                }
                if ("null".equals(id)) {
                    return new NullLiteral();
                }
                if ("?".equals(id)) {
                    paramCount++;
                    return new ParamLiteral();
                }
                return new IdLiteral(id);
            }
            case LITERAL_FLOAT: {
                Literal literal = new DecimalLiteral((BigDecimal) lexer.decimalValue());
                lexer.nextToken();
                return literal;
            }
            case LITERAL_INT: {
                Literal literal = new IntegerLiteral(BigInteger.valueOf(lexer.integerValue().longValue()));
                lexer.nextToken();
                return literal;
            }

            case LITERAL_HEX: {
                Literal literal = new StringLiteral(lexer.hexString());
                lexer.nextToken();
                return literal;
            }
            case LITERAL_NCHARS:
            case LITERAL_CHARS: {
                Literal literal = new StringLiteral(lexer.stringVal());
                lexer.nextToken();
                return literal;
            }
            case LPAREN: {
                return parentheresExpr();
            }
            case LBRACKET: {
                return arrayExprExpr();
            }
            case EOF:
                throw new ParserException(lexer.info());
        }

    }

    private boolean rightIsExpr(int prec, Precedence next) {
        if (next.leftAssoc) {
            return prec < next.value;
        }
        return prec <= next.value;
    }

    public List<ParseNode> statementList() {
        List<ParseNode> list = new ArrayList<>();
        while (!lexer.isEOF()) {
            list.add(statement());
        }
        return list;
    }

    public static class Precedence {
        private String opText;
        int value;
        boolean leftAssoc; // left associative

        public Precedence(String opText, int v, boolean a) {
            this.opText = opText;
            value = v;
            leftAssoc = a;
        }
    }

    private ParenthesesExpr parentheresExpr() {
        lexer.nextToken();
        List<ParseNode> exprs = new ArrayList<>(3);
        Token token1 = lexer.token();
        if (token1 == Token.RPAREN) {
            lexer.nextToken();
            return new ParenthesesExpr(Collections.emptyList());
        }
        ParseNode expression = expression();
        exprs.add(expression);
        while (true) {
            if (lexer.token() == Token.RPAREN) {
                lexer.nextToken();
                return new ParenthesesExpr(exprs);
            } else if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                exprs.add(expression());
            } else {
                throw new ParserException(lexer.info());
            }
        }

    }

    private CallExpr arrayExprExpr() {
        String funName = "array";
        lexer.nextToken();
        List<ParseNode> exprs = new ArrayList<>(3);
        Token token1 = lexer.token();
        if (token1 == Token.RBRACKET) {
            lexer.nextToken();
            return new CallExpr(funName, new ParenthesesExpr(Collections.emptyList()));
        }
        ParseNode expression = expression();
        exprs.add(expression);
        while (true) {
            if (lexer.token() == Token.RBRACKET) {
                lexer.nextToken();
                return new CallExpr(funName, new ParenthesesExpr(exprs));
            } else if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                exprs.add(expression());
            } else {
                throw new ParserException(lexer.info());
            }
        }
    }

    public int getParamCount() {
        return paramCount;
    }
}