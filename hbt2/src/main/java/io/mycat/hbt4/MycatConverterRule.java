package io.mycat.hbt4;

import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

/**
 * Abstract base class for rule that converts to Mycat.
 */
public abstract class MycatConverterRule extends ConverterRule {
    protected final MycatConvention out;

   public  <R extends RelNode> MycatConverterRule(Class<R> clazz,
                                           Predicate<? super R> predicate, RelTrait in, MycatConvention out,
                                           RelBuilderFactory relBuilderFactory, String description) {
        super(clazz, predicate, in, out, relBuilderFactory, description);
        this.out = out;
    }
}