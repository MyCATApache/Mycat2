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
package io.mycat.hbt.ast.explain;

import io.mycat.hbt.ast.base.Identifier;

/**
 * @author jamie12221
 **/
public class ExplainOp {
    public static void main(String[] args) {
        explan(ExplainType.WITH_IMPLEMENTATION, ExplainAttributes.EXCLUDING_ATTRIBUTES, "JSON");
    }

    public static ExplainStatement explan(ExplainType explainType, ExplainAttributes excludingAttributes, String outType) {
        return new ExplainStatement(explainType, excludingAttributes, outType);
    }

    public static ExplainStatement explan(String explainType, String excludingAttributes, String outType) {
        return explan(ExplainType.valueOf(explainType), ExplainAttributes.valueOf(excludingAttributes), outType);
    }

    public static ExplainStatement explan(Identifier explainType, Identifier excludingAttributes, Identifier outType) {
        return explan(explainType.getValue(), excludingAttributes.getValue(), outType.getValue());
    }
}