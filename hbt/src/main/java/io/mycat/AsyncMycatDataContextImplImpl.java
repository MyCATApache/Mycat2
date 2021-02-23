package io.mycat;

import io.mycat.calcite.CodeExecuterContext;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.observables.ConnectableObservable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.RelNode;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AsyncMycatDataContextImplImpl extends NewMycatDataContextImpl {
    private final IdentityHashMap<RelNode, List<Observable<Object[]>>> viewMap;

    public AsyncMycatDataContextImplImpl(MycatDataContext dataContext,
                                         CodeExecuterContext context,
                                         IdentityHashMap<RelNode, List<Observable<Object[]>>> map,
                                         List<Object> params,
                                         boolean forUpdate) {
        super(dataContext, context, params, forUpdate);

        this.viewMap = new IdentityHashMap<>();
        IdentityHashMap<RelNode, Integer> mycatViews = codeExecuterContext.getMycatViews();
        for (Map.Entry<RelNode, List<Observable<Object[]>>> entry : map.entrySet()) {
            RelNode key = entry.getKey();
            List<Observable<Object[]>> observableList = entry.getValue().stream().map(i -> {
                ConnectableObservable<Object[]> replay = i.replay();
                return replay.autoConnect();
            }).collect(Collectors.toList());
            this.viewMap.put(key, observableList);
        }
    }


    @Override
    public Enumerable<Object[]> getEnumerable(RelNode node) {
        Observable<Object[]> observable = getObservable(node);
        Iterable<Object[]> iterable = observable.toList().blockingGet();
        return Linq4j.asEnumerable(iterable);
    }

    @Override
    public List<Enumerable<Object[]>> getEnumerables(RelNode node) {
        return getObservables(node).stream().map(i -> Linq4j.asEnumerable(i.blockingIterable())).collect(Collectors.toList());
    }

    @Override
    public Observable<Object[]> getObservable(RelNode node) {
        List<Observable<Object[]>> observables = getObservables(node);
        if (observables.size() == 1) {
            return observables.get(0);
        }
        return Observable.merge(observables);
    }

    @Override
    public List<Observable<Object[]>> getObservables(RelNode node) {
        return viewMap.get(node);
    }
}
