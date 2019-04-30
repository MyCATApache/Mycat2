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

public enum MySQLPayloadType {
    UNKNOWN,
    ERROR,
    OK,
    EOF,
    COLUMN_COUNT,
    CHARACTER_SET,
    PREPARE_OK,
    COLUMN_DEFINITION,
    TEXT_RESULTSET_ROW,
    BINARY_RESULTSET_ROW,
    LOCAL_INFILE_REQUEST,
    LOCAL_INFILE_CONTENT_OF_FILENAME,
    LOCAL_INFILE_EMPTY_PACKET,
    SEND_LONG_DATA,
    COM_STMT_CLOSE,
}
