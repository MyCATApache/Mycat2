
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


package io.ordinate.engine.vector;


import io.mycat.beans.mycat.ArrowTypes;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.Arrays;
import java.util.List;

public class AddLongLongExpression extends AbstractVectorExpression {

    private List<VectorExpression> children;

    public AddLongLongExpression( List<VectorExpression> children) {
        super(ArrowTypes.INT64_TYPE);
        this.children = children;
    }

    public static AddLongLongExpression of(List<VectorExpression> vExpressions){
        return new AddLongLongExpression(vExpressions);
    }

    @Override
    public void eval(VectorContext ctx) {
        BigIntVector output = (BigIntVector) ctx.getOutputVector();
        BigIntVector leftVector = (BigIntVector) ctx.getLeftVector();
        BigIntVector rightVector = (BigIntVector) ctx.getRightVector();
        final int rowCount = ctx.getRowCount();
        for (int i = 0; i < rowCount; i++) {
            output.set(i,leftVector.get(i)+rightVector.get(i));
        }
    }

    @Override
    public String signature() {
        return null;
    }

    @Override
    public List<ArrowType> argTypes() {
        return Arrays.asList(ArrowTypes.INT64_TYPE,ArrowTypes.INT64_TYPE);
    }

    public  static long add(long l, long r) {
        return l + r;
    }
}
