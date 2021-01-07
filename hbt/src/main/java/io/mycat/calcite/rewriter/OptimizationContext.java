package io.mycat.calcite.rewriter;

import io.mycat.calcite.MycatRel;
import io.mycat.calcite.spm.PlanCache;
import io.mycat.calcite.spm.PlanImpl;
import lombok.Getter;

@Getter
public class OptimizationContext {
    boolean complex = false;
    boolean predicateOnView = false;
    boolean parameterized = false;

    public OptimizationContext() {
    }


    public void saveAlways() {
        parameterized = true;
    }

    public void setPredicateOnView(boolean b) {
        this.predicateOnView = b;
    }

    public void saveParameterized() {
        saveAlways();
    }
}