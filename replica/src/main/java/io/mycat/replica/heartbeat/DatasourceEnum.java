/**
 * Copyright (C) <2021>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.replica.heartbeat;

/**
 * todo 重构拆分状态
 */
public enum DatasourceEnum {

    OK_STATUS(1),
    ERROR_STATUS(-1),
    TIMEOUT_STATUS(-2),
    INIT_STATUS(0);
    final int value;

    DatasourceEnum(int value) {
        this.value = value;
    }
}