///**
// * Copyright (C) <2020>  <chen junwen>
// * <p>
// * This program is free software: you can redistribute it and/or modify it under the terms of the
// * GNU General Public License as published by the Free Software Foundation, either version 3 of the
// * License, or (at your option) any later version.
// * <p>
// * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
// * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// * General Public License for more details.
// * <p>
// * You should have received a copy of the GNU General Public License along with this program.  If
// * not, see <http://www.gnu.org/licenses/>.
// */
//package io.mycat.hbt4.logical.rules;
//
//
//import io.mycat.hbt4.MycatConvention;
//import io.mycat.hbt4.MycatConverterRule;
//import io.mycat.hbt4.MycatRules;
//import io.mycat.hbt4.logical.rel.MycatProject;
//import io.mycat.hbt4.logical.rel.MycatUpdateRel;
//import org.apache.calcite.rel.RelNode;
//import org.apache.calcite.rel.core.Project;
//import org.apache.calcite.rel.logical.LogicalTableModify;
//import org.apache.calcite.rex.RexNode;
//import org.apache.calcite.tools.RelBuilderFactory;
//
///**
// * Rule to convert a {@link Project} to
// * an {@link MycatTableModifyRule}.
// */
//public class MycatTableModifyRule extends MycatConverterRule {
//
//    /**
//     * Creates a MycatProjectRule.
//     */
//    public MycatTableModifyRule(final MycatConvention out,
//                                RelBuilderFactory relBuilderFactory) {
//        super(LogicalTableModify.class, project ->
//                        true,
//                MycatRules.convention, out, relBuilderFactory, "MycatTableModifyRule");
//    }
//
//    public RelNode convert(RelNode rel) {
//        final LogicalTableModify project = (LogicalTableModify) rel;
//        return MycatUpdateRel.create(
//                convert(project.getInput(),out),
//                project.getProjects(),
//                project.getRowType());
//    }
//}
//
