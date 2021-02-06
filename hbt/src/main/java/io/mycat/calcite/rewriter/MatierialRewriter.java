package io.mycat.calcite.rewriter;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.MycatRel;
import io.mycat.calcite.executor.MycatBatchNestedLoopJoin;
import io.mycat.calcite.physical.*;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;

import java.util.Collections;
import java.util.List;

public class MatierialRewriter extends RelShuttleImpl {
    @Override
    public RelNode visit(RelNode other) {
        RelNode relNode = super.visit(other);
      return   matierial(relNode);
    }

    public static RelNode matierial(RelNode parent) {
        if (parent instanceof MycatCorrelate) {
            MycatCorrelate mycatCorrelate = (MycatCorrelate) parent;
            return matierial(mycatCorrelate);
        } else if (parent instanceof MycatHashJoin) {
            MycatHashJoin mycatHashJoin = (MycatHashJoin) parent;
            return matierial(mycatHashJoin);
        } else if (parent instanceof MycatNestedLoopJoin) {
            MycatNestedLoopJoin mycatNestedLoopJoin = (MycatNestedLoopJoin) parent;
            return matierial(mycatNestedLoopJoin);
        } else if (parent instanceof MycatNestedLoopSemiJoin) {
            MycatNestedLoopSemiJoin mycatNestedLoopJoin = (MycatNestedLoopSemiJoin) parent;
            return matierial(mycatNestedLoopJoin);
        } else if (parent instanceof MycatBatchNestedLoopJoin) {
            MycatBatchNestedLoopJoin mycatBatchNestedLoopJoin = (MycatBatchNestedLoopJoin) parent;
            return matierial(mycatBatchNestedLoopJoin);
        } else if (parent instanceof MycatFilter) {
            MycatFilter filter = (MycatFilter) parent;
            if(hasCorVal(Collections.singletonList(filter.getCondition()))){
                return matierial(filter);
            }
            return filter;
        } else if (parent instanceof MycatProject) {
            MycatProject project = (MycatProject) parent;
            if(hasCorVal(project.getProjects())){
                return matierial(project);
            }
            return project;
        } else if (parent instanceof MycatCalc) {
            MycatCalc calc = (MycatCalc) parent;
            RexProgram program = calc.getProgram();
            if( hasCorVal(program.getExprList())){
                return matierial(calc);
            }else {
                return calc;
            }
        }
        return parent;
    }

    private static boolean hasCorVal(List<RexNode> rexNodeList) {
        final RelOptUtil.VariableUsedVisitor vuv =
                new RelOptUtil.VariableUsedVisitor(null);
        for (RexNode expr : rexNodeList) {
            expr.accept(vuv);
        }
        return !vuv.variables.isEmpty();
    }

    private static RelNode matierial(MycatCalc calc) {
        MatierialDetector matierialDetector = new MatierialDetector();
        calc.getInput().accept(matierialDetector);
        if (!matierialDetector.isMatierial()) {
            return calc.copy(calc.getTraitSet(),
                    ImmutableList.of(
                            MycatMatierial.create((MycatRel) calc.getInput())));
        } else {
            return calc;
        }
    }
    private static RelNode matierial(MycatFilter calc) {
        MatierialDetector matierialDetector = new MatierialDetector();
        calc.getInput().accept(matierialDetector);
        if (!matierialDetector.isMatierial()) {
            return calc.copy(calc.getTraitSet(),
                    ImmutableList.of(
                            MycatMatierial.create((MycatRel) calc.getInput())));
        } else {
            return calc;
        }
    }
    private  static RelNode matierial(MycatProject calc) {
        MatierialDetector matierialDetector = new MatierialDetector();
        calc.getInput().accept(matierialDetector);
        if (!matierialDetector.isMatierial()) {
            return calc.copy(calc.getTraitSet(),
                    ImmutableList.of(
                            MycatMatierial.create((MycatRel) calc.getInput())));
        } else {
            return calc;
        }
    }
    private  static RelNode matierial(MycatBatchNestedLoopJoin mycatBatchNestedLoopJoin) {
        MatierialDetector matierialDetector = new MatierialDetector();
        mycatBatchNestedLoopJoin.getRight().accept(matierialDetector);
        if (!matierialDetector.isMatierial()) {
            return mycatBatchNestedLoopJoin.copy(mycatBatchNestedLoopJoin.getTraitSet(),
                    ImmutableList.of(mycatBatchNestedLoopJoin.getLeft(),
                            MycatMatierial.create((MycatRel) mycatBatchNestedLoopJoin.getRight())));
        } else {
            return mycatBatchNestedLoopJoin;
        }
    }

    private static RelNode matierial(MycatNestedLoopSemiJoin mycatNestedLoopSemiJoin) {
        MatierialDetector matierialDetector = new MatierialDetector();
        mycatNestedLoopSemiJoin.getRight().accept(matierialDetector);
        if (!matierialDetector.isMatierial()) {
            return mycatNestedLoopSemiJoin.copy(mycatNestedLoopSemiJoin.getTraitSet(),
                    ImmutableList.of(mycatNestedLoopSemiJoin.getLeft(),
                            MycatMatierial.create((MycatRel) mycatNestedLoopSemiJoin.getRight())));
        } else {
            return mycatNestedLoopSemiJoin;
        }
    }

    private static RelNode matierial(MycatCorrelate mycatCorrelate) {
        MatierialDetector matierialDetector = new MatierialDetector();
        mycatCorrelate.getRight().accept(matierialDetector);
        if (!matierialDetector.isMatierial()) {
            return mycatCorrelate.copy(mycatCorrelate.getTraitSet(),
                    ImmutableList.of(mycatCorrelate.getLeft(),
                            MycatMatierial.create((MycatRel) mycatCorrelate.getRight())));
        } else {
            return mycatCorrelate;
        }
    }

    private static RelNode matierial(MycatNestedLoopJoin nestedLoopJoin) {
        MatierialDetector matierialDetector = new MatierialDetector();
        nestedLoopJoin.getRight().accept(matierialDetector);
        if (!matierialDetector.isMatierial()) {
            Join copy = nestedLoopJoin.copy(nestedLoopJoin.getTraitSet(),
                    ImmutableList.of(nestedLoopJoin.getLeft(),
                            MycatMatierial.create((MycatRel) nestedLoopJoin.getRight())));
            return copy;
        } else {
            return nestedLoopJoin;
        }
    }

    private static RelNode matierial(MycatHashJoin mycatHashJoin) {
//        MatierialDetector matierialDetector = new MatierialDetector();
//        mycatHashJoin.getRight().accept(matierialDetector);
//        if (!matierialDetector.isMatierial()) {
//            return mycatHashJoin.copy(mycatHashJoin.getTraitSet(),
//                    ImmutableList.of(mycatHashJoin.getLeft(),
//                            MycatMatierial.create((MycatRel) mycatHashJoin.getRight())));
//        } else {
            return mycatHashJoin;
//        }
    }

    public static class MatierialDetector extends RelShuttleImpl {
        boolean matierial = false;

        @Override
        public RelNode visit(RelNode other) {
            if (other instanceof MycatMatierial) {
                matierial = true;
            }
            return super.visit(other);
        }

        public boolean isMatierial() {
            return matierial;
        }
    }

}
