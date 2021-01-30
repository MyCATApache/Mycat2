package io.mycat;

import io.mycat.calcite.CodeExecuterContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.RelNode;
import org.slf4j.LoggerFactory;

import java.util.IdentityHashMap;
import java.util.List;

public class JdbcMycatDataContextImpl extends NewMycatDataContextImpl {
    private final IdentityHashMap<RelNode, List<Enumerable<Object[]>>> viewMap;
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(NewMycatDataContextImpl.class);

    public JdbcMycatDataContextImpl(MycatDataContext dataContext,
                                    CodeExecuterContext context,
                                    IdentityHashMap<RelNode, List<Enumerable<Object[]>>> viewMap,
                                    List<Object> params,
                                    boolean forUpdate) {
        super(dataContext, context, params, forUpdate);
        this.viewMap = viewMap;
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

    /**
     * 获取MergeSort的迭代器
     *
     * @param node
     * @return
     */
    @Override
    public List<Enumerable<Object[]>> getEnumerables(RelNode node) {
        return viewMap.get(node);
    }


}
