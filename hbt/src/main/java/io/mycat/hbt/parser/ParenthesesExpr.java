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

import lombok.Getter;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author jamie12221
 **/
@Getter
public class ParenthesesExpr implements ParseNode {

    private final List<ParseNode> exprs;
    public ParenthesesExpr(ParseNode... exprs){
    this.exprs = Arrays.asList(exprs);
    }
    public ParenthesesExpr(List<ParseNode> exprs) {
        this.exprs = exprs;
    }
    public ParenthesesExpr(ParseNode exprs) {
        this.exprs = Collections.singletonList(exprs);
    }

    @Override
    public void accept(ParseNodeVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    @Override
    public ParenthesesExpr copy() {
        return new ParenthesesExpr((ParseNode) exprs.stream().map(i -> i.copy()).collect(Collectors.toList()));
    }

    @Override
    public String toString() {
        return MessageFormat.format( "({0})",  exprs.stream().map(i->Objects.toString(i)).collect(Collectors.joining(",")));
    }
}