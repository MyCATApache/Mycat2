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

package io.ordinate.engine.function;

import org.apache.calcite.avatica.util.ByteString;
import org.jetbrains.annotations.NotNull;
import scala.util.control.Exception;

public class BinarySequenceImpl implements BinarySequence {
    final byte[] body;

    public BinarySequenceImpl(byte[] body) {
        this.body = body;
    }

    @Override
    public int length() {
        return body.length;
    }

    @Override
    public byte byteAt(long index) {
        return body[(int)index];
    }

    @Override
    public byte[] getBytes() {
        return body;
    }

    @Override
    public int compareTo(@NotNull BinarySequence o) {
        return ByteString.toString(body,16).compareTo(ByteString.toString(o.getBytes(),16));
    }
}
