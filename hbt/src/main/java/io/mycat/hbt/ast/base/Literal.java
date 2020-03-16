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
package io.mycat.hbt.ast.base;

import io.mycat.hbt.HBTOp;
import lombok.Data;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * @author jamie12221
 **/
@Data
public class Literal extends Expr {
    final Object value;

    public Literal(Object value) {
        super(HBTOp.LITERAL);
        if (value instanceof Double) {
            this.value = BigDecimal.valueOf((Double) value);
        } else if (value instanceof Float) {
            this.value = BigDecimal.valueOf((Float) value);
        } else if (value instanceof Integer) {
            this.value = BigInteger.valueOf((Integer) value);
        } else if (value instanceof Long) {
            this.value = BigInteger.valueOf((Long) value);
        } else {
            this.value = value;
        }
    }

    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }
}
