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
package io.mycat.hbt.ast.base;

import io.mycat.hbt.ast.HBTOp;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author jamie12221
 **/
@Data
@EqualsAndHashCode
public class Fun extends Expr {
    final String functionName;

    public Fun(String functionName,List<Expr> nodes) {
        super(HBTOp.FUN, nodes);
        this.functionName = functionName;
    }

    @Override
    public String toString() {
        return functionName + "(" +
                nodes.stream().map(i -> i.toString()).collect(Collectors.joining(",")) +
                ')';
    }
}