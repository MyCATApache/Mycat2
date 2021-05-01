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
import java.util.Objects;

/**
 * @author jamie12221
 **/
@Getter
public class CallExpr implements ParseNode {
    private final String name;
    private final ParenthesesExpr args;

    public CallExpr(String name, ParenthesesExpr args) {
        this.name = name;
        this.args = args;
    }


    @Override
    public void accept(ParseNodeVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    @Override
    public CallExpr copy() {
        return new CallExpr(name, args.copy());
    }

    @Override
    public String toString() {
        return MessageFormat.format( "{0}{1}",name,Objects.toString(args));
    }

    public String getName() {
        return name;
    }
}