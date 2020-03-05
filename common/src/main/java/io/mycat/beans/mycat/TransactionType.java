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

package io.mycat.beans.mycat;

/**
 * @author jamie12221
 * date 2020-01-09 23:18
 **/
public enum TransactionType {
    PROXY_TRANSACTION_TYPE("proxy"),
    JDBC_TRANSACTION_TYPE("xa"),
    ;

    private String name;

    TransactionType(String name) {
        this.name = name;
    }

    public static final TransactionType DEFAULT = TransactionType.JDBC_TRANSACTION_TYPE;

    public static TransactionType parse(String name) {
        return TransactionType.JDBC_TRANSACTION_TYPE.name.equalsIgnoreCase(name) ? TransactionType.JDBC_TRANSACTION_TYPE : TransactionType.PROXY_TRANSACTION_TYPE;
    }
}