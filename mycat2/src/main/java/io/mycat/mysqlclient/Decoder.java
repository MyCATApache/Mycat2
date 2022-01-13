/**
 * Copyright (C) <2022>  <chen junwen>
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

package io.mycat.mysqlclient;

import io.mycat.vertx.ReadView;
import io.vertx.core.buffer.Buffer;
import io.vertx.mysqlclient.MySQLException;

public interface Decoder<T> {
    void initColumnCount(int count);

    void addColumn(int index,Buffer buffer);

    T convert(Buffer payload);

    void onColumnEnd();

    default Throwable convertException(Buffer payload) {
        ReadView readView = new ReadView(payload);
        readView.skipInReading(1);
        int errorCode = (int) readView.readFixInt(2);
        readView.skipInReading(1);
        String sqlState = readView.readFixString(5);
        String errorMessage = readView.readEOFString();
        return (new MySQLException(errorMessage, errorCode, sqlState));
    }
}