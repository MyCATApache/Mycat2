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

public enum ComQueryState {
    //DO_NOT(false),
    QUERY_PACKET(true),
    FIRST_PACKET(true),
    COLUMN_DEFINITION(false),
    COLUMN_END_EOF(true),
    RESULTSET_ROW(false),
    RESULTSET_ROW_END(true),
    PREPARE_FIELD(false),
    PREPARE_FIELD_EOF(true),
    PREPARE_PARAM(false),
    PREPARE_PARAM_EOF(true),
    COMMAND_END(false),
//    LOCAL_INFILE_REQUEST(true),
    LOCAL_INFILE_FILE_CONTENT(true),
//    LOCAL_INFILE_EMPTY_PACKET(true),
    LOCAL_INFILE_OK_PACKET(true);
    boolean needFull;

    public boolean isNeedFull() {
        return needFull;
    }

    ComQueryState(boolean needFull) {
        this.needFull = needFull;
    }
}