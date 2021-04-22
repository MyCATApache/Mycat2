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
package io.mycat.calcite.physical;


import io.mycat.calcite.ExplainWriter;
import io.mycat.calcite.MycatConvention;
import io.mycat.calcite.MycatRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.util.RxBuiltInMethod;

import java.util.Objects;

public class MycatMatierial extends SingleRel implements MycatRel {

    private final MycatRel input;

    protected MycatMatierial(RelOptCluster cluster, RelTraitSet traitSet, MycatRel input) {
        super(cluster, Objects.requireNonNull(traitSet).replace(MycatConvention.INSTANCE), input);
        this.input = input;
        this.rowType = input.getRowType();
    }
    public MycatMatierial(RelInput relInput) {
        this(relInput.getCluster(), relInput.getTraitSet(),(MycatRel) relInput.getInput());
    }
    public static final MycatMatierial create( MycatRel input){
        return create(input.getCluster(),input.getTraitSet(),input);
    }
    public static final MycatMatierial create(RelOptCluster cluster, RelTraitSet traits, MycatRel input) {
        return new MycatMatierial(cluster, traits, input);
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatMatierial").into();
        for (RelNode relNode : getInputs()) {
            MycatRel relNode1 = (MycatRel) relNode;
            relNode1.explain(writer);
        }
        return writer.ret();
    }

    @Override
    public boolean isSupportStream() {
        return false;
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        final BlockBuilder builder = new BlockBuilder();
        final Result result =
                input.implement(implementor, pref);
        Expression input = builder.append("child", result.block);
        final Expression childExp = toEnumerate(input);
        builder.add(Expressions.call(RxBuiltInMethod.ENUMERABLE_MATIERIAL.method, childExp));
        return implementor.result(result.physType, builder.toBlock());
    }
}
