package io.mycat.calcite;

import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.util.JsonUtil;
import io.mycat.vertx.DataNodeMapping;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Rel implements Serializable {
    public MycatRowMetaData columnInfo;
    public int count = 0;
    public DataNodeMapping dataNodeMapping;

//    public String toJson(){
//        Map<String,String> map = new HashMap<>();
//        map.put("count",String.valueOf(count));
//        map.put("dataNodeMapping",dataNodeMapping.toJson());
//        map.put("columnInfo",columnInfo.toJson());
//        return JsonUtil.toJson(map);
//    }
}