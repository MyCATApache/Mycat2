package io.mycat.calcite;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;

public class MycatRexBuilder extends RexBuilder {
    /**
     * Creates a RexBuilder.
     *
     * @param typeFactory Type factory
     */
    public MycatRexBuilder(RelDataTypeFactory typeFactory) {
        super(typeFactory);
    }
}