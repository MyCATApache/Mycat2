
/*
 *     Copyright (C) <2021>  <Junwen Chen>
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package io.ordinate.engine.function.math;


import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.*;
import io.ordinate.engine.record.Record;

import java.util.List;

public class AddDoubleFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "+(double,double)";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new AddDoubleFunc((ScalarFunction) args.get(0), (ScalarFunction) args.get(1));
    }

    private static class AddDoubleFunc extends DoubleFunction implements BinaryArgFunction {
        final ScalarFunction left;
        final ScalarFunction right;
        boolean isNull;

        public AddDoubleFunc(ScalarFunction left, ScalarFunction right) {
            super();
            this.left = left;
            this.right = right;
        }

        @Override
        public double getDouble(Record rec) {
            final double left = this.left.getDouble(rec);
            final double right = this.right.getDouble(rec);
            isNull = this.left.isNull(rec) || this.right.isNull(rec);
            if (isNull) return 0;
            return left + right;
        }

        @Override
        public boolean isConstant() {
            return left.isConstant() && right.isConstant()
                    || (left.isConstant())
                    || (right.isConstant());
        }

        @Override
        public ScalarFunction getLeft() {
            return left;
        }

        @Override
        public ScalarFunction getRight() {
            return right;
        }

        @Override
        public boolean isNull(Record rec) {
            return isNull;
        }
    }
}
