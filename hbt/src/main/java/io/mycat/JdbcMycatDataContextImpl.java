package io.mycat;

import io.mycat.calcite.CodeExecuterContext;
import io.reactivex.rxjava3.core.Observable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.rel.RelNode;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JdbcMycatDataContextImpl extends NewMycatDataContextImpl {
    private final IdentityHashMap<RelNode, List<Enumerable<Object[]>>> viewMap;
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(NewMycatDataContextImpl.class);

    public JdbcMycatDataContextImpl(MycatDataContext dataContext,
                                    CodeExecuterContext context,
                                    IdentityHashMap<RelNode, List<Enumerable<Object[]>>> viewMap,
                                    List<Object> params,
                                    boolean forUpdate) {
        super(dataContext, context, params, forUpdate);
        this.viewMap = new IdentityHashMap<>();
        IdentityHashMap<RelNode, Integer> mycatViews = codeExecuterContext.getMycatViews();
        for (Map.Entry<RelNode, List<Enumerable<Object[]>>> entry : viewMap.entrySet()) {
            RelNode key = entry.getKey();
            List<Enumerable<Object[]>> observableList = entry.getValue().stream().map(i -> {
                if (mycatViews.get(key) > 1) {
                    return Linq4j.asEnumerable(i.toList());
                } else {
                    return i;
                }
            }).collect(Collectors.toList());
            this.viewMap.put(key, observableList);
        }
    }

    /**
     * 获取mycatview的迭代器
     *
     * @param node
     * @return
     */
    @Override
    public Enumerable<Object[]> getEnumerable(RelNode node) {
        List<Enumerable<Object[]>> enumerables = viewMap.get(node);
        if (enumerables.size() == 1) {
            return enumerables.get(0);
        }
        return Linq4j.concat(enumerables);
    }

    @Override
    public Enumerable<Object[]> getEnumerable(RelNode node, Function1 function1, Comparator comparator, int offset, int fetch) {
        return null;
    }

    @Override
    public Observable<Object[]> getObservable(RelNode node) {
        return null;
    }

    @Override
    public Observable<Object[]> getObservable(RelNode relNode, Function1 function1, Comparator comparator, int offset, int fetch) {
        return null;
    }



}
