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
package io.mycat;

import java.util.List;

public interface TextUpdateInfo {
    public static TextUpdateInfo create(String targetName, List<String> sqls) {
        return new TextUpdateInfoImpl(targetName, sqls);
    }

    String targetName();

    List<String> sqls();

    public class TextUpdateInfoImpl implements TextUpdateInfo {
        final String targetName;
        final List<String> sqls;

        public TextUpdateInfoImpl(String targetName, List<String> sqls) {
            this.targetName = targetName;
            this.sqls = sqls;
        }


        @Override
        public String targetName() {
            return targetName;
        }

        @Override
        public List<String> sqls() {
            return sqls;
        }
    }
}
