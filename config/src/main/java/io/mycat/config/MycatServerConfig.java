package io.mycat.config;

import io.mycat.util.JsonUtil;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.Map;

@Data
@EqualsAndHashCode
public class MycatServerConfig {
    LoadBalance loadBalance = new LoadBalance();
    ServerConfig server = new io.mycat.config.ServerConfig ();
    String mode = "local";
    String datasourceProvider = null;
    Map<String, Object> properties = new HashMap<>();

    public static void main(String[] args) {
        MycatServerConfig mycatServerConfig = new MycatServerConfig();
        System.out.println(JsonUtil.toJson(mycatServerConfig));
    }

}