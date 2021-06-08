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

    public MycatViewDataNodeMappingImpl(boolean containsOrder, List<String> uniqueTableNames, IndexCondition indexCondition) {
        this.containsOrder = containsOrder;
        this.distribution = Distribution.of(uniqueTableNames);
        this.indexCondition = indexCondition;
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
    public Stream<Map<String, Partition>> apply(List<Object> objects) {
        return distribution.getDataNodes(table -> IndexCondition.getObject(table.getShardingFuntion(), indexCondition, objects));
    }

    public Distribution distribution() {
        return distribution;
    }

    @Override
    public String toJson() {
        Map<String,String> map = new HashMap<>();
        map.put("containsOrder",containsOrder?"true":"false");
        map.put("uniqueTableNames", JsonUtil.toJson(uniqueTableNames));
        map.put("indexCondition", indexCondition.toJson());
        return JsonUtil.toJson(map);
    }
}
