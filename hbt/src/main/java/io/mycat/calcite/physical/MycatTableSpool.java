package io.mycat.calcite.physical;

import io.mycat.calcite.ExplainWriter;
import io.mycat.calcite.MycatConvention;
import io.mycat.calcite.MycatRel;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.core.TableSpool;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.util.BuiltInMethod;

public class MycatTableSpool extends TableSpool implements MycatRel {

    private MycatTableSpool(RelOptCluster cluster, RelTraitSet traitSet,
                                 RelNode input, Type readType, Type writeType, RelOptTable table) {
        super(cluster, traitSet, input, readType, writeType, table);
    }

    /** Creates an EnumerableTableSpool. */
    public static MycatTableSpool create(RelNode input, Type readType,
                                              Type writeType, RelOptTable table) {
        RelOptCluster cluster = input.getCluster();
        RelMetadataQuery mq = cluster.getMetadataQuery();
        RelTraitSet traitSet = cluster.traitSetOf(MycatConvention.INSTANCE)
                .replaceIfs(RelCollationTraitDef.INSTANCE,
                        () -> mq.collations(input))
                .replaceIf(RelDistributionTraitDef.INSTANCE,
                        () -> mq.distribution(input));
        return new MycatTableSpool(cluster, traitSet, input, readType, writeType, table);
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatTableSpool").into();
        for (RelNode relNode : getInputs()) {
            MycatRel relNode1 = (MycatRel) relNode;
            relNode1.explain(writer);
        }
        return writer.ret();
    }


    @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        // TODO for the moment only LAZY read & write is supported
        if (readType != Type.LAZY || writeType != Type.LAZY) {
            throw new UnsupportedOperationException(
                    "EnumerableTableSpool supports for the moment only LAZY read and LAZY write");
        }

        //  ModifiableTable t = (ModifiableTable) root.getRootSchema().getTable(tableName);
        //  return lazyCollectionSpool(t.getModifiableCollection(), <inputExp>);

        BlockBuilder builder = new BlockBuilder();

        RelNode input = getInput();
        Result inputResult = implementor.visitChild(this, 0, (EnumerableRel) input, pref);

        String tableName = table.getQualifiedName().get(table.getQualifiedName().size() - 1);
        Expression tableExp = Expressions.convert_(
                Expressions.call(
                        Expressions.call(
                                implementor.getRootExpression(),
                                BuiltInMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method),
                        BuiltInMethod.SCHEMA_GET_TABLE.method,
                        Expressions.constant(tableName, String.class)),
                ModifiableTable.class);
        Expression collectionExp = Expressions.call(
                tableExp,
                BuiltInMethod.MODIFIABLE_TABLE_GET_MODIFIABLE_COLLECTION.method);

        Expression inputExp = toEnumerate(builder.append("input", inputResult.block));

        Expression spoolExp = Expressions.call(
                BuiltInMethod.LAZY_COLLECTION_SPOOL.method,
                collectionExp,
                inputExp);
        builder.add(spoolExp);

        PhysType physType = PhysTypeImpl.of(
                implementor.getTypeFactory(),
                getRowType(),
                pref.prefer(inputResult.format));
        return implementor.result(physType, builder.toBlock());
    }

    @Override protected Spool copy(RelTraitSet traitSet, RelNode input,
                                   Type readType, Type writeType) {
        return new MycatTableSpool(input.getCluster(), traitSet, input,
                readType, writeType, table);
    }
}
