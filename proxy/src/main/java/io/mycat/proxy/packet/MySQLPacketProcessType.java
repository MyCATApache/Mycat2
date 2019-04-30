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

public enum  MySQLPacketProcessType {
    REQUEST(),
    LOAD_DATA_REQUEST,
    REQUEST_SEND_LONG_DATA(),
    REQUEST_COM_STMT_CLOSE(),
    FIRST_ERROR(),
    FIRST_OK(),
    FIRST_EOF,
    COLUMN_COUNT,
    COLUMN_DEF,
    COLUMN_EOF,
    TEXT_ROW,
    BINARY_ROW,
    ROW_EOF,
    ROW_FINISHED,
    ROW_OK,
    ROW_ERROR,
    PREPARE_OK,
    PREPARE_OK_PARAMER_DEF,
    PREPARE_OK_COLUMN_DEF,
    PREPARE_OK_COLUMN_DEF_EOF,
    PREPARE_OK_PARAMER_DEF_EOF;

}
