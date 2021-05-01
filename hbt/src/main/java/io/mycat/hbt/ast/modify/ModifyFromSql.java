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
package io.mycat.hbt.ast.modify;

import io.mycat.hbt.ast.base.NodeVisitor;
import io.mycat.hbt.ast.base.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import static io.mycat.hbt.ast.HBTOp.MODIFY_FROM_SQL;

/**
 * @author jamie12221
 **/
@Getter
@EqualsAndHashCode
public class ModifyFromSql extends Schema {
    private final String targetName;
    private final String sql;

    public ModifyFromSql(String targetName,String sql) {
        super(MODIFY_FROM_SQL);
        this.targetName = targetName;
        this.sql = sql;
    }
    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }
}