package io.mycat.calcite.logical;

import io.mycat.*;
import io.mycat.calcite.rewriter.Distribution;
import io.mycat.calcite.rewriter.IndexCondition;
import io.mycat.util.JsonUtil;
import lombok.Getter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@Getter
public class MycatViewDataNodeMappingImpl implements MycatViewDataNodeMapping {
    transient Distribution distribution;
    boolean containsOrder;
    List<String> uniqueTableNames;
    IndexCondition indexCondition;
    private ViewInfo viewInfo;

    public MycatViewDataNodeMappingImpl(boolean containsOrder, List<String> uniqueTableNames, IndexCondition indexCondition, ViewInfo viewInfo) {
        this.containsOrder = containsOrder;
        this.distribution = Distribution.of(uniqueTableNames);
        this.indexCondition = indexCondition;
        this.viewInfo = viewInfo;
    }

    @Override
    public boolean containsOrder() {
        return containsOrder;
    }

    @Override
    public Distribution.Type getType() {
        return this.distribution.type();
    }

    @Override
    public Stream<Map<String, DataNode>> apply(List<Object> objects) {
        return distribution.getDataNodes(table -> IndexCondition.getObject(table.getShardingFuntion(), indexCondition, objects));
    }

    public Distribution distribution() {
        return distribution;
    }

    @Override
    public ViewInfo viewInfo() {
        return viewInfo;
    }

//    @Override
//    public String toJson() {
//        Map<String,String> map = new HashMap<>();
//        map.put("containsOrder",containsOrder?"true":"false");
//        map.put("uniqueTableNames", JsonUtil.toJson(uniqueTableNames));
//        map.put("indexCondition", indexCondition.toJson());
//        return JsonUtil.toJson(map);
//    }

    public ViewInfo getViewInfo() {
        return viewInfo;
    }
}
