package io.mycat.hbt4;

import com.google.common.collect.ImmutableList;
import io.mycat.hbt3.MultiView;
import io.mycat.hbt3.Part;
import io.mycat.hbt3.PartInfo;
import io.mycat.hbt3.View;
import io.mycat.hbt4.executor.MycatUnionAllExecutor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.util.SqlString;
import org.jetbrains.annotations.NotNull;

import java.util.List;

public class ExecutorImplementorImpl extends BaseExecutorImplementor {
    private final DatasourceFactory factory;

    public ExecutorImplementorImpl(List<Object> context, DatasourceFactory factory) {
        super(context);
        this.factory = factory;
    }

    @Override
    public Executor implement(MultiView multiView) {
        PartInfo dataNode = multiView.getDataNode();
        Part[] parts = dataNode.toPartArray();
        Executor[] executors = new Executor[parts.length];
        int i = 0;
        for (Part part : parts) {
            RelNode relNode = multiView.getRelNode();
            SqlString sql = part.getSql(relNode);
            Object[] objects1 = getPzarameters(sql.getDynamicParameters());
            executors[i++] = factory.create(part.getMysqlIndex(),sql.getSql(), objects1);
        }
        return new MycatUnionAllExecutor(executors);
    }

    @Override
    public Executor implement(View view) {
        Part part = view.getDataNode().getPart(0);
        SqlString sql = part.getSql(view.getRelNode());
        ImmutableList<Integer> dynamicParameters = sql.getDynamicParameters();
        Object[] objects = getPzarameters(dynamicParameters);
        return factory.create(part.getMysqlIndex(),sql.getSql(), objects);
    }

    @NotNull
    public Object[] getPzarameters(ImmutableList<Integer> dynamicParameters) {
        Object[] objects;
        if (dynamicParameters!=null){
            objects =dynamicParameters.stream().map(i -> context.get(i)).toArray();
        }else {
            objects = new Object[]{};
        }
        return objects;
    }
}