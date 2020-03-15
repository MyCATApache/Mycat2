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
package io.mycat.calcite;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.rules.FilterSetOpTransposeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.sql.SqlDialect;

/**
 * @author Junwen Chen
 **/
public class MycatConvention extends Convention.Impl {


    public final SqlDialect dialect;
    public final String targetName;

    public MycatConvention(String targetName,SqlDialect dialect) {
        super("MYCAT2."+targetName, MycatRel.class);
        this.dialect = dialect;
        this.targetName = targetName;
    }

    public static MycatConvention of(String targetName,SqlDialect dialect) {
        return new MycatConvention(targetName,dialect);
    }

    @Override
    public void register(RelOptPlanner planner) {
        planner.addRule(FilterSetOpTransposeRule.INSTANCE);
        planner.addRule(ProjectRemoveRule.INSTANCE);
    }
}
