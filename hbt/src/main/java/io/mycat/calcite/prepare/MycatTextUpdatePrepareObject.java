package io.mycat.calcite.prepare;

import io.mycat.api.collector.MergeUpdateRowIterator;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.UpdateRowIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.PrepareMycatRowMetaData;
import io.mycat.beans.mycat.UpdateRowMetaData;
import io.mycat.calcite.MycatCalciteContext;
import io.mycat.hbt.ast.modify.MergeModify;
import io.mycat.hbt.ast.modify.ModifyFromSql;
import io.mycat.upondb.UponDBContext;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;


public class MycatTextUpdatePrepareObject extends PrepareObject {

    private final int variantRefCount;
    private final BiFunction<MycatTextUpdatePrepareObject, List, Iterator<TextUpdateInfo>> textUpdateInfoProvider;
    private final UponDBContext dbContext;

    public MycatTextUpdatePrepareObject(Long id, int variantRefCount, BiFunction<MycatTextUpdatePrepareObject, List, Iterator<TextUpdateInfo>> textUpdateInfoProvider, UponDBContext dbContext) {
        super(id);
        this.variantRefCount = variantRefCount;
        this.textUpdateInfoProvider = textUpdateInfoProvider;
        this.dbContext = dbContext;
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
            public List<String> explain() {
                MergeModify mergeModify = getMergeModify(params);
                return ExpainObject.explain(null, MycatCalciteContext.INSTANCE.convertToHBTText(mergeModify),null);
            }

            @Override
            public RowBaseIterator run() {
                Iterator<TextUpdateInfo> iterator = textUpdateInfoProvider.apply(MycatTextUpdatePrepareObject.this, params);
                return new MergeUpdateRowIterator(new Iterator<UpdateRowIterator>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public UpdateRowIterator next() {
                        TextUpdateInfo next = iterator.next();
                        return dbContext.update(next.targetName(),next.sqls());
                    }
                });
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