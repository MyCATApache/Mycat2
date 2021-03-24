/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mycat.calcite;

import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.physical.*;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;

import java.util.Set;

public class MycatRelFieldTrimmer extends RelFieldTrimmer {
    private RelBuilder relBuilder;

    public MycatRelFieldTrimmer(SqlValidator validator, RelBuilder relBuilder) {
        super(validator, relBuilder);
        this.relBuilder = relBuilder;
    }

    /**
     * TrimResult.class,
     * this,
     * "trimFields",
     * RelNode.class,
     * ImmutableBitSet.class,
     * Set.class
     * final ImmutableBitSet fieldsUsed,
     * Set<RelDataTypeField> extraFields
     *
     * @param
     * @return
     */
    public TrimResult trimFields(MycatView rel, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
        MycatView view = (MycatView) rel;
        RelNode oldRelNode = view.getRelNode();
        RelNode newRelNode = RelOptUtil.createProject(oldRelNode, fieldsUsed.asList());
        if (newRelNode == oldRelNode) {
            return super.trimFields(rel, fieldsUsed, extraFields);
        }
        LogicalProject project = (LogicalProject) newRelNode;
        Mappings.TargetMapping mapping = project.getMapping();
        if (mapping==null||project.getProjects().isEmpty()){
            return super.trimFields(rel, fieldsUsed, extraFields);
        }
        MycatView newMycatView = new MycatView(view.getTraitSet(), newRelNode, view.getDistribution());
        return super.result(newMycatView, Mappings.target(mapping, rel.getRowType().getFieldCount(), newMycatView.getRowType().getFieldCount()));
    }

    public TrimResult trimFields(MycatMergeSort rel, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
        return super.trimFields(rel, fieldsUsed, extraFields);
    }

    public TrimResult trimFields(MycatHashJoin join, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
        return super.trimFields(join, fieldsUsed, extraFields);
    }

    public TrimResult trimFields(MycatSortAgg mycatSortAgg, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
        return super.trimFields(mycatSortAgg, fieldsUsed, extraFields);
    }

    public TrimResult trimFields(MycatCalc relNode, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
        return super.trimFields(relNode, fieldsUsed, extraFields);
    }

    public TrimResult trimFields(MycatCorrelate relNode, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
        return super.trimFields(relNode, fieldsUsed, extraFields);
    }

    public TrimResult trimFields(MycatFilter relNode, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
        return super.trimFields(relNode, fieldsUsed, extraFields);
    }

    public TrimResult trimFields(MycatGather relNode, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
        return super.trimFields(relNode, fieldsUsed, extraFields);
    }

    public TrimResult trimFields(MycatHashAggregate relNode, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
        return super.trimFields(relNode, fieldsUsed, extraFields);
    }

    public TrimResult trimFields(MycatInsertRel relNode, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
        return super.trimFields(relNode, fieldsUsed, extraFields);
    }

    public TrimResult trimFields(MycatIntersect relNode, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
        return super.trimFields(relNode, fieldsUsed, extraFields);
    }

    public TrimResult trimFields(MycatLookUpView relNode, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
        return super.trimFields(relNode, fieldsUsed, extraFields);
    }

    public TrimResult trimFields(MycatMatierial relNode, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
        return super.trimFields(relNode, fieldsUsed, extraFields);
    }

    public TrimResult trimFields(MycatMemSort relNode, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
        return super.trimFields(relNode, fieldsUsed, extraFields);
    }

    public TrimResult trimFields(MycatMinus relNode, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
        return super.trimFields(relNode, fieldsUsed, extraFields);
    }

    public TrimResult trimFields(MycatNestedLoopJoin relNode, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
        return super.trimFields(relNode, fieldsUsed, extraFields);
    }

    public TrimResult trimFields(MycatNestedLoopSemiJoin relNode, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
        return super.trimFields(relNode, fieldsUsed, extraFields);
    }

    public TrimResult trimFields(MycatProject relNode, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
        return super.trimFields(relNode, fieldsUsed, extraFields);
    }

    public TrimResult trimFields(MycatQuery relNode, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
        return super.trimFields(relNode, fieldsUsed, extraFields);
    }

    public TrimResult trimFields(MycatSemiHashJoin relNode, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
        return super.trimFields(relNode, fieldsUsed, extraFields);
    }

    public TrimResult trimFields(MycatSortMergeJoin relNode, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
        return super.trimFields(relNode, fieldsUsed, extraFields);
    }

    public TrimResult trimFields(MycatSortMergeSemiJoin relNode, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
        return super.trimFields(relNode, fieldsUsed, extraFields);
    }

    public TrimResult trimFields(MycatTableModify relNode, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
        return super.trimFields(relNode, fieldsUsed, extraFields);
    }

    public TrimResult trimFields(MycatTopN relNode, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
        return super.trimFields(relNode, fieldsUsed, extraFields);
    }

    public TrimResult trimFields(MycatUnion relNode, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
        return super.trimFields(relNode, fieldsUsed, extraFields);
    }

    public TrimResult trimFields(MycatUpdateRel relNode, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
        return super.trimFields(relNode, fieldsUsed, extraFields);
    }

    public TrimResult trimFields(MycatValues relNode, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
        return super.trimFields(relNode, fieldsUsed, extraFields);
    }

    public TrimResult trimFields(QueryView relNode, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
        return super.trimFields(relNode, fieldsUsed, extraFields);
    }

    public TrimResult trimFields(MycatWindow relNode, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
        return super.trimFields(relNode, fieldsUsed, extraFields);
    }


    @Override
    public TrimResult trimFields(RelNode rel, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
        return super.trimFields(rel, fieldsUsed, extraFields);
    }
}
