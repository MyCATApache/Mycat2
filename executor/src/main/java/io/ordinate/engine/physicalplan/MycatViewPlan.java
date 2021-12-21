package io.ordinate.engine.physicalplan;

import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.physical.MycatMergeSort;
import io.mycat.calcite.rewriter.Distribution;
import io.ordinate.engine.builder.CalciteCompiler;
import io.ordinate.engine.builder.PhysicalSortProperty;
import io.ordinate.engine.factory.FactoryUtil;
import io.ordinate.engine.function.IntFunction;
import io.ordinate.engine.record.RootContext;
import io.reactivex.rxjava3.core.Observable;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.NewMycatDataContext;
import org.apache.calcite.sql.SqlKind;

import java.util.Collections;
import java.util.List;

public class MycatViewPlan implements PhysicalPlan {
    final MycatView mycatView;
    final Schema schema;
    private IntFunction offset;
    private IntFunction fecth;

    public MycatViewPlan(MycatView mycatView, IntFunction offset,IntFunction fecth) {
        this.mycatView = mycatView;
        this.schema = FactoryUtil.toArrowSchema(mycatView.getMycatRelDataTypeByCalcite());
        this.offset = offset;
        this.fecth = fecth;
    }

    @Override
    public Schema schema() {
        return this.schema;
    }

    @Override
    public List<PhysicalPlan> children() {
        return Collections.emptyList();
    }

    @Override
    public Observable<VectorSchemaRoot> execute(RootContext rootContext) {
        NewMycatDataContext context = (NewMycatDataContext) rootContext.getContext();
        MycatView view = (MycatView) mycatView;
        Schema schema = FactoryUtil.toArrowSchema(view.getMycatRelDataTypeByCalcite());
        if (view.getDistribution().type() == Distribution.Type.SHARDING) {
            if (view.getRelNode() instanceof Sort) {
                Sort viewRelNode = (Sort) view.getRelNode();

                Integer offset;
                Integer fetch;
                if (viewRelNode.offset != null || viewRelNode.fetch != null) {
                    offset = this.offset.getInt(null);
                    fetch =  this.fecth.getInt(null);
                    List<Observable<Object[]>> observableList = context.getObservableList(view.getDigest());
                    RelCollation collation = view.getTraitSet().getCollation();
                    List<PhysicalSortProperty> physicalSortProperties = CalciteCompiler.getPhysicalSortProperties(viewRelNode);
                    return new MergeSortObjectArray(schema, observableList, physicalSortProperties, offset, fetch).execute(rootContext);

                }
                {
                    List<Observable<Object[]>> observableList = context.getObservableList(view.getDigest());
                    List<PhysicalSortProperty> physicalSortProperties = CalciteCompiler.getPhysicalSortProperties(viewRelNode);
                    return new MergeSortObjectArray(schema, observableList, physicalSortProperties, 0, Integer.MAX_VALUE).execute(rootContext);
                }
            }
        }
        return new UnionAllObjectArray(schema, context.getObservableList(view.getDigest())).execute(rootContext);
    }

    @Override
    public void accept(PhysicalPlanVisitor physicalPlanVisitor) {

    }
}
