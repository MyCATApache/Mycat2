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
import lombok.Data;

import java.text.MessageFormat;

@Data
public class BaselineAddHint extends HintBuilder {
    String sql;
    boolean fix;
    public static String create(String sql){
        return create(false,sql);
    }
    public static String create(boolean fix,String sql) {
        BaselineAddHint addHint = new BaselineAddHint();
        addHint.setFix(fix);
        addHint.setSql(sql);
        return addHint.build();
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    @Override
    public String getCmd() {
        return fix?"BASELINE FIX ":"BASELINE ADD ";
    }

    @Override
    public String build() {
        return getCmd() +
                sql;
    }
}