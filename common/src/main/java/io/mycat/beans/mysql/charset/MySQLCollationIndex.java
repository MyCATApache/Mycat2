/**
 * Copyright (C) <2019>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.beans.mysql.charset;

import java.util.HashMap;
import java.util.Map;


/**
 * @author jamie12221
 *  date 2019-05-05 16:22
 *
 * mysql 状态 字符集 INDEX_TO_CHARSET
 **/
public class MySQLCollationIndex {
    /**
     * collationIndex 和 charsetName 的映射
     */
    public final Map<Integer, String> INDEX_TO_CHARSET = new HashMap<>();

    public void put(Integer index, String charset) {
        INDEX_TO_CHARSET.put(index, charset);
    }

    public String getCharsetByIndex(Integer integer) {
        return INDEX_TO_CHARSET.get(integer);
    }

    public boolean isEmpty() {
        return INDEX_TO_CHARSET.isEmpty();
    }

}
