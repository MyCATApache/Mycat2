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

/**
 * @author jamie12221
 *  date 2019-05-05 16:22
 *
 * mysql 状态 autocommit
 **/
public enum MySQLAutoCommit {
    ON("1", "SET autocommit = 1;"),
    OFF("0", "SET autocommit = 0;");

    MySQLAutoCommit(String text, String cmd) {
        this.text = text;
        this.cmd = cmd;
    }

    private String text;
    private String cmd;

    /**
     * Getter for property 'text'.
     *
     * @return Value for property 'text'.
     */
    public String getText() {
        return text;
    }

    public String getCmd() {
        return cmd;
    }
}
