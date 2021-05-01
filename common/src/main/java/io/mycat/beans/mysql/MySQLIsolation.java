/**
 * Copyright (C) <2021>  <yannan>
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
package io.mycat.beans.mysql;

import java.sql.Connection;
import java.util.Optional;

public enum MySQLIsolation {
    READ_UNCOMMITTED("READ UNCOMMITTED", "SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;",
            Connection.TRANSACTION_READ_UNCOMMITTED),
    READ_COMMITTED("READ COMMITTED", "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;",
            Connection.TRANSACTION_READ_COMMITTED),
    REPEATED_READ("REPEATABLE READ", "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;",
            Connection.TRANSACTION_REPEATABLE_READ),
    SERIALIZABLE("SERIALIZABLE", "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE;",
            Connection.TRANSACTION_SERIALIZABLE),

    ;
    public static MySQLIsolation DEFAULT = REPEATED_READ;

    static {

    }

    private final String text;
    private final String cmd;
    private final int jdbcValue;

    MySQLIsolation(String text, String cmd, int jdbcValue) {
        this.text = text;
        this.cmd = cmd;
        this.jdbcValue = jdbcValue;
    }

    public static MySQLIsolation parse(String name) {
        for (MySQLIsolation value : values()) {
            if (value.getText().equalsIgnoreCase(name)) {
                return value;
            }
        }
        return null;
    }

    public static MySQLIsolation parseJdbcValue(int name) {
        switch (name) {
            case Connection.TRANSACTION_READ_UNCOMMITTED: {
                return READ_UNCOMMITTED;
            }
            case Connection.TRANSACTION_READ_COMMITTED: {
                return READ_COMMITTED;
            }
            case Connection.TRANSACTION_REPEATABLE_READ: {
                return REPEATED_READ;
            }
            case Connection.TRANSACTION_SERIALIZABLE: {
                return SERIALIZABLE;
            }
        }
        throw new UnsupportedOperationException();
    }

    public String getText() {
        return text;
    }

    public String getCmd() {
        return cmd;
    }

    public int getJdbcValue() {
        return jdbcValue;
    }

    public static Optional<MySQLIsolation> toMySQLIsolationFrom(String sql) {
        for (MySQLIsolation value : values()) {
            if (value.getCmd() == sql) {
                return Optional.of(value);
            }
        }
        return Optional.empty();
    }
}