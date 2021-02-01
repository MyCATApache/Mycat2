package io.mycat;

import io.mycat.calcite.CodeExecuterContext;
import io.reactivex.rxjava3.core.Observable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.RelNode;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.stream.Collectors;

public class AsyncMycatDataContextImplImpl extends NewMycatDataContextImpl {
    private final IdentityHashMap<RelNode, List<Observable<Object[]>>> viewMap;

    public AsyncMycatDataContextImplImpl(MycatDataContext dataContext,
                                         CodeExecuterContext context,
                                         IdentityHashMap<RelNode, List<Observable<Object[]>>> map,
                                         List<Object> params,
                                         boolean forUpdate) {
        super(dataContext, context, params, forUpdate);
        this.viewMap = map;
    }


    @Override
    public Enumerable<Object[]> getEnumerable(RelNode node) {
        return Linq4j.asEnumerable(Observable.concat(getObservables(node)).blockingIterable());
    }

    @Override
    public List<Enumerable<Object[]>> getEnumerables(RelNode node) {
        return getObservables(node).stream().map(i -> Linq4j.asEnumerable(i.blockingIterable())).collect(Collectors.toList());
    }

    @Override
    public Observable<Object[]> getObservable(RelNode node) {
        return (Observable<Object[]>) viewMap.get(node).get(0);
    }

    @Override
    public List<Observable<Object[]>> getObservables(RelNode node) {
        return (List) viewMap.get(node);
    }
}
