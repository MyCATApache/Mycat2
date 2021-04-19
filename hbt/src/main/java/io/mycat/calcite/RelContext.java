package io.mycat.calcite;

import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.util.JsonUtil;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class RelContext implements Serializable {
    public final Map<String, Rel> nodes = new HashMap<>();
    public MycatRel relNode;
    public boolean forUpdate;


    public RelContext(MycatRel relNode, boolean forUpdate) {
        this.relNode = relNode;
        this.forUpdate = forUpdate;
    }

//    public String toJson() {
//        Map<String,String> map = new HashMap<>();
//        map.put("forUpdate",forUpdate?"true":"false");
//
//        Map<String,String> nodeMap = new HashMap<>();
//        for (Map.Entry<String, Rel> stringRelEntry : nodes.entrySet()) {
//            nodeMap.put(stringRelEntry.getKey(),stringRelEntry.getValue().toJson());
//        }
//
//        map.put("nodes", JsonUtil.toJson(nodeMap));
//        return JsonUtil.toJson(map);
//    }
}

