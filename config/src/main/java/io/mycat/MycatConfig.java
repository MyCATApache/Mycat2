package io.mycat;

import io.mycat.config.*;
import io.mycat.util.YamlUtil;
import lombok.Data;

@Data
public class MycatConfig {
    PatternRootConfig pattern = new PatternRootConfig();
    ShardingQueryRootConfig sharding = new ShardingQueryRootConfig();
    DatasourceRootConfig datasource = new DatasourceRootConfig();
    ReplicasRootConfig replicas = new ReplicasRootConfig();
    SecurityConfig security = new SecurityConfig();
    ServerConfig server = new ServerConfig();
    PlugRootConfig plug = new PlugRootConfig();


    public static void main(String[] args) {
        MycatConfig mycatConfig = new MycatConfig();
        String dump = YamlUtil.dump(mycatConfig);
        System.out.println(dump);
    }
}