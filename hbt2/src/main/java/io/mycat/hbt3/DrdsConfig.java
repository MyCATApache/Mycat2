package io.mycat.hbt3;

import com.google.common.collect.ImmutableList;
import io.mycat.util.JsonUtil;
import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class DrdsConfig implements DrdsConst {
     int shardingSchemaNum = 8;
     int datasourceNum = 1;
     boolean autoCreateTable = true;
     boolean planCache = false;
     Map<String, List<String>> schemas = new HashMap<>();

    public static void main(String[] args) {
        DrdsConfig drdsConfig = new DrdsConfig();
        Map<String, List<String>> schemas = drdsConfig.getSchemas();
        schemas.put("db1", ImmutableList.of("CREATE TABLE `travelrecord` ( `id` bigint(20) NOT NULL AUTO_INCREMENT,`user_id` varchar(100) CHARACTER SET utf8 DEFAULT NULL,`traveldate` date DEFAULT NULL,`fee` decimal(10,0) DEFAULT NULL,`days` int(11) DEFAULT NULL,`blob` longblob DEFAULT NULL) dbpartition by hash(id)"));
        String s = JsonUtil.toJson(drdsConfig);
        System.out.println(s);

    }
}