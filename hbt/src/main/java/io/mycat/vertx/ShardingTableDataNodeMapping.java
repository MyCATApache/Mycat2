//package io.mycat.vertx;
//
//import io.mycat.MetaClusterCurrent;
//import io.mycat.calcite.logical.MycatView;
//import io.mycat.calcite.logical.MycatViewDataNodeMapping;
//import io.mycat.calcite.logical.MycatViewSqlString;
//import io.mycat.config.ServerConfig;
//import io.mycat.util.JsonUtil;
//import lombok.AllArgsConstructor;
//import lombok.NoArgsConstructor;
//import org.apache.calcite.sql.SqlNode;
//
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//@NoArgsConstructor
//
//public class ShardingTableDataNodeMapping implements DataNodeMapping {
//    MycatViewDataNodeMapping mycatViewDataNodeMapping;
//    SqlNode sqlTemplate;
//
//    public ShardingTableDataNodeMapping(MycatViewDataNodeMapping mycatViewDataNodeMapping, SqlNode sqlTemplate) {
//        this.mycatViewDataNodeMapping = mycatViewDataNodeMapping;
//        this.sqlTemplate = sqlTemplate;
//    }
//
//    @Override
//    public MycatViewSqlString apply(List<Object> objects) {
//        int mergeUnionSize = MetaClusterCurrent.exist(ServerConfig.class) ? MetaClusterCurrent.wrapper(ServerConfig.class).getMergeUnionSize() : 5;
//        return MycatView.apply(mycatViewDataNodeMapping, sqlTemplate, objects, mergeUnionSize);
//    }
//
////    @Override
////    public String toJson() {
////        Map<String,String> map = new HashMap<>();
////        map.put("sqlTemplate",sqlTemplate);
////        map.put("mycatViewDataNodeMapping",mycatViewDataNodeMapping.toJson());
////        return JsonUtil.toJson(map);
////    }
//}
