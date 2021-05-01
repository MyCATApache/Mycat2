/**
 * Copyright (C) <2021>  <chen junwen>
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

import io.mycat.hbt.parser.literal.*;

/**
 * @author jamie12221
 **/
public interface ParseNodeVisitor {

    void visit(Bind bind);

    void endVisit(Bind bind);

    void visit(CallExpr call);

    void endVisit(CallExpr call);

    void visit(IdLiteral id);

    void endVisit(IdLiteral id);

    void visit(ParenthesesExpr parenthesesExpr);

    void endVisit(ParenthesesExpr parenthesesExpr);

    void visit(IntegerLiteral numberLiteral);

    void endVisit(IntegerLiteral numberLiteral);

    void visit(StringLiteral stringLiteral);

    void endVisit(StringLiteral stringLiteral);

    void visit(DecimalLiteral decimalLiteral);

    void endVisit(DecimalLiteral decimalLiteral);

    void visit(BooleanLiteral booleanLiteral);

    void endVisit(BooleanLiteral booleanLiteral);

    void visit(NullLiteral nullLiteral);

    void endVisit(NullLiteral nullLiteral);

    void visit(ParamLiteral paramLiteral);

    void endVisit(ParamLiteral paramLiteral);
}