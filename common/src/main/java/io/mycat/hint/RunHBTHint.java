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
package io.mycat.hint;

import io.mycat.util.JsonUtil;

import java.text.MessageFormat;
import java.util.Collections;

public class RunHBTHint extends HintBuilder {
    private String hbt;

    public static String create(String hbt) {
        RunHBTHint createDataSourceHint = new RunHBTHint();
        createDataSourceHint.setHbt(hbt);
        return createDataSourceHint.build();
    }

    public void setHbt(String hbt) {
        this.hbt = hbt;
    }

    @Override
    public String getCmd() {
        return "run";
    }

    @Override
    public String build() {
        return MessageFormat.format("/*+ mycat:{0}{1} */;",
                getCmd(),
                JsonUtil.toJson(Collections.singletonMap("hbt", hbt)));
    }
}