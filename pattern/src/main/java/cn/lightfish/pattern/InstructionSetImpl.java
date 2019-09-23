/**
 * Copyright (C) <2019>  <chen junwen>
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
package cn.lightfish.pattern;

import java.util.Map;
import java.util.Objects;

/**
 * https://github.com/junwen12221/GPattern.git
 *
 * @author Junwen Chen
 **/
public class InstructionSetImpl implements InstructionSet {
    public static String toUpperCase(Map map, Object key) {
        return Objects.toString(map.get(key)).toUpperCase();
    }

    public static Byte one() {
        return Byte.valueOf((byte) 1);
    }

    public static String getNameAsString(DynamicSQLMatcher matcher, String key) {
        return matcher.getResult().getName(key);
    }
}