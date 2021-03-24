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
package io.mycat.calcite.rules;


import io.mycat.calcite.MycatConvention;
import io.mycat.calcite.MycatConverterRule;
import io.mycat.calcite.MycatRules;
import io.mycat.calcite.physical.MycatProject;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * Rule to convert a {@link Project} to
 * an {@link MycatProjectRule}.
 */
public class MycatProjectRule extends MycatConverterRule {

    /**
     * Creates a MycatProjectRule.
     */
    public MycatProjectRule(final MycatConvention out,
                            RelBuilderFactory relBuilderFactory) {
        super(Project.class, project ->
                        !project.containsOver(),
                MycatRules.IN_CONVENTION, out, relBuilderFactory, "MycatProjectRule");
    }

    public RelNode convert(RelNode rel) {
        final Project project = (Project) rel;
        return MycatProject.create(
                convert(project.getInput(),out),
                project.getProjects(),
                project.getRowType());
    }
}

