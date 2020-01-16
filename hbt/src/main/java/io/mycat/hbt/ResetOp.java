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
package io.mycat.hbt;

import io.mycat.hbt.ast.base.Identifier;
import io.mycat.hbt.ast.base.Node;
import io.mycat.hbt.ast.other.ResetStatement;

import static io.mycat.hbt.LevelType.SESSION;

/**
 * @author jamie12221
 **/
public interface ResetOp {

    static ResetStatement reset(LevelType leve1, String identifier) {
        return new ResetStatement(leve1, identifier);
    }

    static ResetStatement reset(String identifier) {
        return reset(SESSION, identifier);
    }

    static ResetStatement reset(LevelType leve1, Node identifier) {
        return reset(leve1, identifier);
    }

    static ResetStatement reset(Node identifier) {
        Identifier id = (Identifier) identifier;
        return reset(id.getValue());
    }
}