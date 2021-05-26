
package io.mycat.calcite.logical;

import io.mycat.calcite.ExplainWriter;
import io.mycat.calcite.MycatConvention;
import io.mycat.calcite.MycatRel;
import io.mycat.calcite.localrel.ToLocalConverter;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;


import java.util.List;


public class LocalToMycatRelConverter
    extends ConverterImpl
    implements MycatRel {
  public LocalToMycatRelConverter(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input) {
    super(cluster, ConventionTraitDef.INSTANCE, traits.replace(MycatConvention.INSTANCE), input);
    this.rowType = input.getRowType();
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new LocalToMycatRelConverter(
        getCluster(), traitSet, sole(inputs));
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    return planner.getCostFactory().makeTinyCost();
  }

  @Override
  public ExplainWriter explain(ExplainWriter writer) {
    return null;
  }
}
