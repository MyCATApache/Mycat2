/**
 * Copyright (C) <2019>  <yannan>
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
package io.mycat.beans.mysql;

public enum MySQLIsolation {
    READ_UNCOMMITTED("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;"),
    READ_COMMITTED("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;"),
    REPEATED_READ("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;"),
    SERIALIZABLE("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE;");

    private String cmd;

    MySQLIsolation(String cmd) {
        this.cmd = cmd;
    }

    public String getCmd() {
        return cmd;
    }
}