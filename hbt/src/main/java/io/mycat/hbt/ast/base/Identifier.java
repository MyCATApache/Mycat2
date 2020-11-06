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

import io.mycat.hbt.ast.HBTOp;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Objects;

/**
 * @author jamie12221
 **/
@Data
@EqualsAndHashCode
public class Identifier extends Expr {
    final String value;

    public Identifier(String value) {
        super(HBTOp.IDENTIFIER);
        this.value = value;
    }

    public boolean isStar() {
        return "*".equalsIgnoreCase(Objects.toString(value));
    }

    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }
}


