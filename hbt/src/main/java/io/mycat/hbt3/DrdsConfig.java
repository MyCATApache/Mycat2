/**
 * Copyright (C) <2020>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
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
        DrdsConst drdsConfig = new DrdsConfig();
        Map<String, List<String>> schemas = drdsConfig.getSchemas();
        schemas.put("db1", ImmutableList.of("CREATE TABLE `travelrecord` ( `id` bigint(20) NOT NULL AUTO_INCREMENT,`user_id` varchar(100) CHARACTER SET utf8 DEFAULT NULL,`traveldate` date DEFAULT NULL,`fee` decimal(10,0) DEFAULT NULL,`days` int(11) DEFAULT NULL,`blob` longblob DEFAULT NULL) dbpartition by hash(id)"));
        String s = JsonUtil.toJson(drdsConfig);
        System.out.println(s);

    }

    @Override
    public MycatTableFactory getMycatTableFactory() {
        return new MycatTableFactory() {
            @Override
            public AbstractMycatTable create(String schemaName, String createTableSql, DrdsConst drdsConst) {
                return new MycatTableAdapter(schemaName,createTableSql,drdsConst);
            }
        };
    }
}