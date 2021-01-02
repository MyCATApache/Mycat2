package io.mycat.calcite;

import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.rex.RexBuilder;

import java.util.Map;

public class MycatEnumerableRelImplementor extends EnumerableRelImplementor {
    public MycatEnumerableRelImplementor(RexBuilder rexBuilder, Map<String, Object> internalParameters) {
        super(rexBuilder, internalParameters);
    }

    public Function1<String, RexToLixTranslator.InputGetter> getAllCorrelateVariablesFunction() {
        return allCorrelateVariables;
    }

}
