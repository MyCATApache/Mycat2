/**
 * Copyright (C) <2021>  <chen junwen>
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
package io.mycat.api.collector;

import java.util.List;
import java.util.Map;

/**
 * @author jamie12221
 * date 2019-05-22 23:18
 * simple result set callback
 **/
public interface CommonSQLCallback {

    List<String> getSqls();

    default void process(List<List<Map<String, Object>>> resultSetList) {
        process(resultSetList,false);
    }

    void process(List<List<Map<String, Object>>> resultSetList, boolean readonly);

    void onException(Throwable e);
}