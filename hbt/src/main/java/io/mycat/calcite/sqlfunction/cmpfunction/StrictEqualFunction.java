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
package io.mycat.calcite.sqlfunction.cmpfunction;

import io.mycat.calcite.MycatScalarFunction;

import org.apache.calcite.mycat.MycatSqlDefinedFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.sql.*;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Objects;

public class StrictEqualFunction extends MycatSqlDefinedFunction {
    public static ScalarFunction scalarFunction = MycatScalarFunction.create(StrictEqualFunction.class,
            "strictEqual", 2);
    public static StrictEqualFunction INSTANCE = new StrictEqualFunction();


    public StrictEqualFunction() {
        super("<=>", scalarFunction, SqlFunctionCategory.SYSTEM);

    }

    public static Boolean strictEqual(BigDecimal b0, BigDecimal b1) {
        if (b0 == null && b1 == null) {
            return true;
        }
        if (b0 == null && b1 != null) {
            return null;
        }
        if (b0 != null && b1 == null) {
            return null;
        }
        return b0.stripTrailingZeros().equals(b1.stripTrailingZeros());
    }

    public static Boolean strictEqualAny(Object left, Object right) {
        if (left == null && right == null) {
            return true;
        }
        if (left == null && right != null) {
            return null;
        }
        if (left != null && right == null) {
            return null;
        }
        return Objects.equals(left, right);
    }

    public static Boolean strictEqual(Object[] left, Object[] right) {
        if (left == null && right == null) {
            return true;
        }
        if (left == null && right != null) {
            return null;
        }
        if (left != null && right == null) {
            return null;
        }
        return Arrays.equals(left, right);
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        SqlUtil.unparseBinarySyntax(this, call, writer, leftPrec, rightPrec);
    }
}
