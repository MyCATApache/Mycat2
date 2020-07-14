package io.mycat.calcite.prepare;

import io.mycat.MycatConnection;
import io.mycat.PlanRunner;
import io.mycat.TextUpdateInfo;
import io.mycat.api.collector.MergeUpdateRowIterator;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.UpdateRowIteratorResponse;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.PrepareMycatRowMetaData;
import io.mycat.beans.mycat.UpdateRowMetaData;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.hbt.ast.modify.MergeModify;
import io.mycat.hbt.ast.modify.ModifyFromSql;
import io.mycat.upondb.MycatDBContext;
import io.mycat.upondb.PrepareObject;
import io.mycat.util.Explains;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.BiFunction;


public class MycatTextUpdatePrepareObject extends PrepareObject {

    private final int variantRefCount;
    private final BiFunction<MycatTextUpdatePrepareObject, List, Iterator<TextUpdateInfo>> textUpdateInfoProvider;
    private final MycatDBContext dbContext;

    public MycatTextUpdatePrepareObject(Long id, int variantRefCount, BiFunction<MycatTextUpdatePrepareObject, List, Iterator<TextUpdateInfo>> textUpdateInfoProvider, MycatDBContext dbContext) {
        super(id,false);
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
                return Explains.explain(null,null,MycatCalciteSupport.INSTANCE.dumpMetaData(resultSetRowType()), MycatCalciteSupport.INSTANCE.convertToHBTText(mergeModify),null);
            }

            @Override
            public RowBaseIterator run() {
                Iterator<TextUpdateInfo> iterator = getTextUpdateInfoIterator(params);
                MergeUpdateRowIterator mergeUpdateRowIterator = new MergeUpdateRowIterator(new Iterator<UpdateRowIteratorResponse>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public UpdateRowIteratorResponse next() {
                        TextUpdateInfo next = iterator.next();
                        String targetName = next.targetName();
                        MycatConnection connection = dbContext.getConnection(targetName);
                        long updateCount = 0;
                        long lastInsertId = 0;
                        for (String sql : next.sqls()) {
                            UpdateRowIteratorResponse mycatUpdateResponse = connection.executeUpdate(sql, true, dbContext.getServerStatus());
                            updateCount += mycatUpdateResponse.getUpdateCount();
                            lastInsertId = Math.max(mycatUpdateResponse.getLastInsertId(), lastInsertId);
                        }
                        return new UpdateRowIteratorResponse(updateCount, lastInsertId, dbContext.getServerStatus());

                    }
                }, dbContext.getServerStatus());
                return mergeUpdateRowIterator;
            }
        };
    }

    private Iterator<TextUpdateInfo> getTextUpdateInfoIterator(List<Object> params) {
        return textUpdateInfoProvider.apply(MycatTextUpdatePrepareObject.this, params);
    }

    public Map<String,List<String>> getRouteMap(){
        HashMap<String,List<String>> map = new HashMap<>();
        Iterator<TextUpdateInfo> textUpdateInfoIterator = getTextUpdateInfoIterator(Collections.emptyList());
        while (textUpdateInfoIterator.hasNext()){
            TextUpdateInfo next = textUpdateInfoIterator.next();
            List<String> strings = map.computeIfAbsent(next.targetName(), (s) -> new ArrayList<>(1));
            strings.addAll(next.sqls());
        }
        return map;
    }


    @NotNull
    private MergeModify getMergeModify(List<Object> params) {
        ArrayList<ModifyFromSql> strings = new ArrayList<>();
        Iterator<TextUpdateInfo> iterator = getTextUpdateInfoIterator(params);
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