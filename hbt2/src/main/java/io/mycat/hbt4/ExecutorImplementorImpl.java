package io.mycat.hbt4;

import io.mycat.hbt3.MultiView;
import io.mycat.hbt3.Part;
import io.mycat.hbt3.PartInfo;
import io.mycat.hbt3.View;

import java.util.Map;

public class ExecutorImplementorImpl extends BaseExecutorImplementor {
    private final DatasourceFactory factory;

    public ExecutorImplementorImpl(Map<String, Object> context, DatasourceFactory factory) {
        super(context);
        this.factory = factory;
    }

    @Override
    public Executor implement(MultiView multiView) {
        PartInfo dataNode = multiView.getDataNode();
        Part[] parts = dataNode.toPartArray();
        return factory.create(parts);
    }

    @Override
    public Executor implement(View view) {
        Part part = view.getDataNode().getPart(0);
        return factory.create(part);
    }
}