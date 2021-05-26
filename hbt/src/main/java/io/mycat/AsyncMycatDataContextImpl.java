package io.mycat;

import io.mycat.calcite.CodeExecuterContext;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.physical.MycatMergeSort;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.observables.ConnectableObservable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.util.RxBuiltInMethodImpl;

import java.util.*;
import java.util.stream.Collectors;

public class AsyncMycatDataContextImpl extends NewMycatDataContextImpl {
    private final Map<String, List<Observable<Object[]>>> viewMap;

    public AsyncMycatDataContextImpl(MycatDataContext dataContext,
                                     CodeExecuterContext context,
                                     Map<String, List<Observable<Object[]>>> map,
                                     List<Object> params,
                                     boolean forUpdate) {
        super(dataContext, context, params, forUpdate);
        this.viewMap = map;
    }


    @Override
    public Enumerable<Object[]> getEnumerable(Map<String, List<SqlString>> map) {
        return null;
    }

    @Override
    public Enumerable<Object[]> getEnumerable(String node) {
        return Linq4j.asEnumerable(getObservable(node).blockingIterable());
    }

    @Override
    public Enumerable<Object[]> getEnumerable(String node, Function1 function1, Comparator comparator, int offset, int fetch) {
        return Linq4j.asEnumerable(getObservable(node, function1, comparator, offset, fetch).blockingIterable());
    }

    @Override
    public Observable<Object[]> getObservable(String node) {
        List<Observable<Object[]>> observables = viewMap.get(node);
        return Observable.merge(observables);
    }

    @Override
    public Observable<Object[]> getObservable(String relNode, Function1 function1, Comparator comparator, int offset, int fetch) {
            return MycatView.streamOrderBy(Objects.requireNonNull(viewMap.get(relNode)), function1, comparator, offset, fetch);
    }

}
