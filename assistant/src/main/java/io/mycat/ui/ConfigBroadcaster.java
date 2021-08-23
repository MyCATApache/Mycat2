package io.mycat.ui;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@EqualsAndHashCode
@Data
@ToString
public class ConfigBroadcaster {
    LoginInfo master = new LoginInfo();
    List<LoginInfo> slaves = new ArrayList<>();
    Type type;

    public static enum Type {
        DB,
        FILE
    }

    @Data
    @EqualsAndHashCode
    public static class LoginInfo {
        String url = "jdbc:mysql://localhost:3306/mysql";
        String user = "root";
        String password = "123456";
    }

    public static String MASTER_DEMO_URL = "jdbc:mysql://localhost:3306/mysql";
    public static String SLAVE_DEMO_URL = "jdbc:mysql://localhost:3307/mysql";

    public static ConfigBroadcaster demo() {
        ConfigBroadcaster configBroadcaster = new ConfigBroadcaster();

        LoginInfo masterLoginInfo = new LoginInfo();
        masterLoginInfo.setUrl(MASTER_DEMO_URL);
        configBroadcaster.setMaster(masterLoginInfo);

        LoginInfo slaveLoginInfo = new LoginInfo();
        slaveLoginInfo.setUrl(SLAVE_DEMO_URL);

        configBroadcaster.setSlaves(Arrays.asList(slaveLoginInfo));
        return configBroadcaster;
    }
}
