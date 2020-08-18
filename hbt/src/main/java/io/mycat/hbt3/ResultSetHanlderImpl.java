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
package io.mycat.hbt3;

import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.mpp.Row;

public class ResultSetHanlderImpl implements ResultSetHanlder {
    @Override
    public void onOk(long lastInsertId, long affectedRow) {

    }

    @Override
    public void onMetadata(MycatRowMetaData mycatRowMetaData) {

    }

    @Override
    public void onRow(Row row) {
        System.out.println(row);
    }

    @Override
    public void onError(Throwable e) {

    }
}