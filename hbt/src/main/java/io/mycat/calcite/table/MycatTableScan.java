package io.mycat.calcite.table;

import io.mycat.TableHandler;
import io.mycat.calcite.ExplainWriter;
import io.mycat.calcite.MycatConvention;
import io.mycat.calcite.MycatEnumerableRelImplementor;
import io.mycat.calcite.MycatRel;
import io.mycat.calcite.rewriter.IndexCondition;
import lombok.Getter;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.NewMycatDataContext;

import java.lang.reflect.Method;
import java.util.Optional;

@Getter
public class MycatTableScan extends AbstractRelNode implements MycatRel {
    final RelNode relNode;
    final RexNode condition;


    public MycatTableScan(RelTraitSet relTrait, RelNode input) {
        this(relTrait, input, null);
    }

    public MycatTableScan(RelInput relInput) {
        this(relInput.getTraitSet(), relInput.getInput(), relInput.getExpression("condition"));
    }

    public MycatTableScan(RelTraitSet relTrait, RelNode input, RexNode conditions) {
        super(input.getCluster(), relTrait = relTrait.replace(MycatConvention.INSTANCE));
        this.condition = conditions;
        this.rowType = input.getRowType();
        this.relNode = input;
    }

    public Optional<IndexCondition> getPredicateIndexCondition() {
        return Optional.empty();
    }

    public static MycatTableScan ofTable(RelNode input) {
        return new MycatTableScan(input.getTraitSet().replace(MycatConvention.INSTANCE), input);
    }

    @Override
    public Result implement(MycatEnumerableRelImplementor implementor, Prefer pref) {
        final BlockBuilder builder = new BlockBuilder();
        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getRowType(),
                        JavaRowFormat.ARRAY);
        ParameterExpression root = implementor.getRootExpression();
        TableScan tableScan = (TableScan) this.relNode;
        MycatLogicTable  mycatLogicTable = (MycatLogicTable) tableScan.getTable().unwrap(AbstractMycatTable.class);
        TableHandler table = mycatLogicTable.getTable();
        Method getObservable = Types.lookupMethod(NewMycatDataContext.class, "getTableObservable", String.class, String.class);
        builder.add(Expressions.call(root, getObservable, Expressions.constant(table.getSchemaName()),Expressions.constant(table.getTableName())));
        return implementor.result(physType, builder.toBlock());
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        RelWriter writer = super.explainTerms(pw);
        writer.item("relNode", relNode.getDigest());
        if (condition != null) {
            writer.item("conditions", condition);
        }
        return writer;
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        return null;
    }
}
