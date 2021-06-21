package io.mycat.calcite.physical;

import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.MycatConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;

public class MycatTableLookupValues extends AbstractRelNode {

    public MycatTableLookupValues(RelOptCluster cluster, RelDataType relDataType, RelTraitSet traitSet) {
        super(cluster, traitSet.replace(MycatConvention.INSTANCE));
        RelDataTypeFactory.FieldInfoBuilder builder = MycatCalciteSupport.TypeFactory.builder();
        int index = 0;
        for (RelDataTypeField relDataTypeField : relDataType.getFieldList()) {
            builder.add("column_"+index,relDataTypeField.getType());
            index++;
        }
        this.rowType = builder.build();
    }

    public MycatTableLookupValues(RelInput input) {
        this(input.getCluster(), input.getRowType("type"), input.getTraitSet());
    }

    public static MycatTableLookupValues create(RelOptCluster cluster, RelDataType relDataType, RelTraitSet traitSet) {
        return new MycatTableLookupValues(cluster, relDataType, traitSet);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw);
    }

}
