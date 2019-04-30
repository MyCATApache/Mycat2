/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.proxy.packet;

public class LongDataPacketImpl implements LongDataPacket {
    long statementId;
    int paramId;
    byte[] data;

    @Override
    public void setLongDataStatementId(long statementId) {
        this.statementId = statementId;
    }

    @Override
    public void setLongDataParamId(int paramId) {
        this.paramId = paramId;
    }

    @Override
    public void setLongData(byte[] longData) {
        this.data = longData;
    }

    @Override
    public long getLongDataStatementId() {
        return statementId;
    }

    @Override
    public long getLongDataParamId() {
        return paramId;
    }

    @Override
    public byte[] getLongData() {
        return data;
    }
}
