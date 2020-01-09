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
package io.mycat.describer.literal;

import io.mycat.describer.ParseNodeVisitor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.math.BigInteger;
import java.util.Objects;

/**
 * @author jamie12221
 **/
@EqualsAndHashCode
@Getter
public class IntegerLiteral implements Literal {
    private BigInteger number;

    public IntegerLiteral(BigInteger number) {
        this.number = number;
    }

    @Override
    public void accept(ParseNodeVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    @Override
    public IntegerLiteral copy() {
        return this;
    }

    @Override
    public String toString() {
        return Objects.toString(number);
    }
}