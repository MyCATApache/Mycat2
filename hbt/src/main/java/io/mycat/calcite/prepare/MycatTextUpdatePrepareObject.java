package io.mycat.calcite.prepare;

import io.mycat.api.collector.MergeUpdateRowIterator;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.UpdateRowIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.PrepareMycatRowMetaData;
import io.mycat.beans.mycat.UpdateRowMetaData;
import io.mycat.calcite.MycatCalciteContext;
import io.mycat.calcite.MycatCalciteDataContext;
import io.mycat.hbt.ast.modify.MergeModify;
import io.mycat.hbt.ast.modify.ModifyFromSql;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;


public class MycatTextUpdatePrepareObject extends MycatPrepareObject {

    private final int variantRefCount;
    private final BiFunction<MycatTextUpdatePrepareObject, List, Iterator<TextUpdateInfo>> textUpdateInfoProvider;

    public MycatTextUpdatePrepareObject(Long id, int variantRefCount, BiFunction<MycatTextUpdatePrepareObject, List, Iterator<TextUpdateInfo>> textUpdateInfoProvider) {
        super(id);
        this.variantRefCount = variantRefCount;
        this.textUpdateInfoProvider = textUpdateInfoProvider;
    }


    @Override
    public MycatRowMetaData prepareParams() {
        return new PrepareMycatRowMetaData(variantRefCount);
    }

    @Override
    public MycatRowMetaData resultSetRowType() {
        return UpdateRowMetaData.INSTANCE;
    }

    @Override
    public PlanRunner plan(List<Object> params) {
        return new PlanRunner() {
            @Override
            public Supplier<RowBaseIterator> run(MycatCalciteDataContext dataContext) {
                return () -> {
                    Iterator<TextUpdateInfo> iterator = textUpdateInfoProvider.apply(MycatTextUpdatePrepareObject.this, params);
                    return new MergeUpdateRowIterator(new Iterator<UpdateRowIterator>() {
                        @Override
                        public boolean hasNext() {
                            return iterator.hasNext();
                        }

                        @Override
                        public UpdateRowIterator next() {
                            TextUpdateInfo next = iterator.next();
                            return dataContext.getUpdateRowIterator(next);
                        }
                    });
                };
            }

            @Override
            public List<String> explain() {
                MergeModify mergeModify = getMergeModify(params);
                return ExpainObject.explain(null, MycatCalciteContext.INSTANCE.convertToHBTText(mergeModify),null);
            }
        };
    }

    @NotNull
    private MergeModify getMergeModify(List<Object> params) {
        ArrayList<ModifyFromSql> strings = new ArrayList<>();
        Iterator<TextUpdateInfo> iterator = textUpdateInfoProvider.apply(MycatTextUpdatePrepareObject.this, params);
        while (iterator.hasNext()) {
            TextUpdateInfo next = iterator.next();
            String key = next.targetName();
            List<String> value = next.sqls();
            for (String s : value) {
                strings.add(new ModifyFromSql(key, s));
            }
        }
        return new MergeModify(strings);
    }
}