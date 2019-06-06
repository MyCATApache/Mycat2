/**
 * Copyright (C) <2019>  <gaozhiwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.config;


import io.mycat.config.datasource.MasterIndexesRootConfig;
import io.mycat.config.datasource.ReplicasRootConfig;
import io.mycat.config.heartbeat.HeartbeatRootConfig;
import io.mycat.config.plug.PlugRootConfig;
import io.mycat.config.proxy.MysqlServerVariablesRootConfig;
import io.mycat.config.proxy.ProxyRootConfig;
import io.mycat.config.route.DynamicAnnotationRootConfig;
import io.mycat.config.route.ShardingRuleRootConfig;
import io.mycat.config.route.SharingFuntionRootConfig;
import io.mycat.config.schema.SchemaRootConfig;
import io.mycat.config.user.UserRootConfig;

/**
 * Desc: 用于指定集群配置文件的枚举值
 *
 * date: 06/09/2017,04/09/2019
 * @author: gaozhiwen chenjunwen
 */
public enum ConfigEnum {
  PROXY(1, "mycat.yml", ProxyRootConfig.class),
  USER(5, "user.yml", UserRootConfig.class),
  DATASOURCE(6, "replicas.yml", ReplicasRootConfig.class),
  REPLICA_INDEX(7, "masterIndexes.yml", MasterIndexesRootConfig.class),
  SCHEMA(8, "schema.yml", SchemaRootConfig.class),
  DYNAMIC_ANNOTATION(9, "dynamicAnnotation.yml", DynamicAnnotationRootConfig.class),
  RULE(10, "rule.yml", ShardingRuleRootConfig.class),
  FUNCTIONS(11, "function.yml", SharingFuntionRootConfig.class),
  PLUG(12, "plug.yml", PlugRootConfig.class),
  HEARTBEAT(12, "heartbeat.yml", HeartbeatRootConfig.class),
  VARIABLES(13, "variables.yml", MysqlServerVariablesRootConfig.class),
  ;
  private byte type;
  private String fileName;
  private Class clazz;

  ConfigEnum(int type, String fileName, Class clazz) {
    this.type = (byte) type;
    this.fileName = fileName;
    this.clazz = clazz;
  }

  public byte getType() {
    return this.type;
  }

  public String getFileName() {
    return this.fileName;
  }

  public Class getClazz() {
    return clazz;
  }

  public static ConfigEnum getConfigEnum(byte type) {
    ConfigEnum[] values = ConfigEnum.values();
    int length = values.length;
    for (int i = 0; i < length; i++) {
      ConfigEnum value = values[i];
      if (values[i].getType() == type) {
        return value;
      }
    }
    throw new RuntimeException("illegal type:" + type);
  }

}
