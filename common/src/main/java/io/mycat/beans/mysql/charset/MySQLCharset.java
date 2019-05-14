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
package io.mycat.beans.mysql.charset;

import java.util.Objects;

/**
 * @author jamie12221
 * @date 2019-05-05 16:22
 *
 * mysql 状态 字符集
 **/
public class MySQLCharset {
    final String charset;
    final int charsetIndex;
    final String setCharsetSQL;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MySQLCharset that = (MySQLCharset) o;
        return charsetIndex == that.charsetIndex &&
                Objects.equals(charset, that.charset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(charset, charsetIndex);
    }

  public String getCharsetCmd() {
        return setCharsetSQL;
    }

    public String getCharset() {
        return charset;
    }

    public int getCharsetIndex() {
        return charsetIndex;
    }

    public MySQLCharset(String charset, int charsetIndex) {
        this.charset = charset;
        this.charsetIndex = charsetIndex;
        this.setCharsetSQL = "SET names " + charset + ";";
    }
}
