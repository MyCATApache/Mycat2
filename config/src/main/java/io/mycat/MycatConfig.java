package io.mycat;

import io.mycat.config.*;
import io.mycat.util.YamlUtil;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class MycatConfig {
    PatternRootConfig interceptor = new PatternRootConfig();
    ShardingQueryRootConfig metadata = new ShardingQueryRootConfig();
    DatasourceRootConfig datasource = new DatasourceRootConfig();
    ClusterRootConfig cluster = new ClusterRootConfig();
    //    SecurityConfig security = new SecurityConfig();
    PlugRootConfig plug = new PlugRootConfig();
    ServerConfig server = new ServerConfig();
    List<String> packageNameList = new ArrayList<>();

    public static void main(String[] args) {
        MycatConfig mycatConfig = new MycatConfig();
        String dump = YamlUtil.dump(mycatConfig);
        System.out.println(dump);
    }
}