package io.mycat.calcite;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptPlanner;

public class ViewConvention extends Convention.Impl {

  public static final ViewConvention INSTANCE = new ViewConvention();

  public static final double COST_MULTIPLIER = 0.8d;

  public ViewConvention() {
    super("VIEW", ViewRel.class);
  }

  @Override public void register(RelOptPlanner planner) {

  }
}
