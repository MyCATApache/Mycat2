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

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public class BinaryExpressionContext implements VectorContext {
    final int leftIndex;
    final int rightIndex;
    final int outputIndex;
    final VectorSchemaRoot input;
    final VectorSchemaRoot output;
    public BinaryExpressionContext(VectorSchemaRoot input,
                                   VectorSchemaRoot output,
                                   int leftIndex,
                                   int rightIndex,
                                   int outputIndex) {
        this.input = input;
        this.output = output;
        this.leftIndex = leftIndex;
        this.rightIndex = rightIndex;
        this.outputIndex = outputIndex;
    }

    @Override
    public VectorSchemaRoot getVectorSchemaRoot() {
        return input;
    }

    public FieldVector getLeftVector() {
        return input.getVector(leftIndex);
    }

    public FieldVector getRightVector() {
        return input.getVector(rightIndex);
    }

    public FieldVector getOutputVector() {
        return output.getVector(outputIndex);
    }

    @Override
    public int getRowCount() {
        return input.getRowCount();
    }

}
