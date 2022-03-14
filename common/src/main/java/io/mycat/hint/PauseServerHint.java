/**
 * Copyright (C) <2022>  <chen junwen>
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
import java.util.ArrayList;
import java.util.List;

@Data
public class PauseServerHint extends HintBuilder {
    List<Long> connectionIds = new ArrayList<>();

    public static String create() {
        return new PauseServerHint().build();
    }

    @Override
    public String getCmd() {
        return "pauseServer";
    }

    @Override
    public String build() {
        return MessageFormat.format("/*+ mycat:{0}{1} */;",
                getCmd(),
                JsonUtil.toJson(this));
    }

    public static void main(String[] args) {
        System.out.println(PauseServerHint.create());
    }
}