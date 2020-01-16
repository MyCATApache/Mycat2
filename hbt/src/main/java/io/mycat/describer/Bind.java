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
package io.mycat.describer;

import lombok.Getter;
import lombok.Setter;

import java.text.MessageFormat;
import java.util.Objects;

/**
 * @author jamie12221
 **/
@Getter
@Setter
public class Bind implements ParseNode {
    String name;
    ParseNode expr;

    public Bind(String name, ParseNode expr) {
        this.name = name;
        this.expr = expr;
    }


    @Override
    public void accept(ParseNodeVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    @Override
    public Bind copy() {
        return new Bind(name, expr.copy());
    }

    @Override
    public String toString() {
        return MessageFormat.format( "let {0} = {1};",Objects.toString(name), Objects.toString(expr));
    }
}