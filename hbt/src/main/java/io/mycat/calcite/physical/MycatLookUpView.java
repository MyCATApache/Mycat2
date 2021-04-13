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

import io.mycat.calcite.*;
import io.mycat.calcite.logical.MycatView;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;

import java.util.List;

public class MycatLookUpView extends AbstractRelNode implements MycatRel {

    private final MycatView relNode;

    public MycatLookUpView(MycatView relNode) {
        super(relNode.getCluster(), relNode.getTraitSet().replace(MycatConvention.INSTANCE));
        this.relNode = relNode;
        this.rowType = relNode.getRowType();
    }


    public static MycatLookUpView create(MycatView relNode){
        return new MycatLookUpView(relNode);
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        return writer.name("MycatLookUpView")
                .item("sql", MycatCalciteSupport.INSTANCE
                        .convertToSqlTemplate(relNode, MycatSqlDialect.DEFAULT,false))
                .into().ret();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw);
    }


    public MycatView getRelNode() {
        return relNode;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MycatLookUpView(relNode);
    }

    @Override
    public boolean isSupportStream() {
        return false;
    }
}