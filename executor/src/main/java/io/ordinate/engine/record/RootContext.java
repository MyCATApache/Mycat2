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

package io.ordinate.engine.record;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.jetbrains.annotations.NotNull;

public class RootContext {
    RootAllocator rootAllocator;

    public RootContext() {
        rootAllocator = new RootAllocator(Long.MAX_VALUE);
    }

    public RootAllocator getRootAllocator() {
        return rootAllocator;
    }

    public int getBatchSize() {
        return 8192 * 4;
    }

    @NotNull
    public VectorSchemaRoot getVectorSchemaRoot(Schema schema) {
        return getVectorSchemaRoot(schema, getBatchSize());
    }

    public VectorSchemaRoot getVectorSchemaRoot(Schema schema, int size) {
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, rootAllocator);
        root.allocateNew();
        return root;
    }
}
