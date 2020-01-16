package io.mycat.metadata;

import io.mycat.MycatConfig;

public class MycatInstance {
    volatile MycatConfig config;

    void with(MycatConfig config) {

    }

//    public static String getPasswordByUserName(MycatConfig config, String userName) {
//        Objects.requireNonNull(config);
//        Objects.requireNonNull(userName);
//        SecurityConfig.UserConfig userConfig = config.getSecurity().getUsers().get(userName);
//        if (userConfig == null) throw new IllegalArgumentException("not exist user name" + userName);
//        String password = userConfig.getPassword();
//        return password == null ? "" : password;
//    }
//
//    public boolean hasUser(MycatConfig config, String userName) {
//        Objects.requireNonNull(config);
//        Objects.requireNonNull(userName);
//        return config.getSecurity().getUsers().containsKey(userName);
//    }

}