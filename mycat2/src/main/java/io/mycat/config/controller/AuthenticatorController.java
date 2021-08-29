package io.mycat.config.controller;

import io.mycat.Authenticator;
import io.mycat.MetaClusterCurrent;
import io.mycat.config.UserConfig;
import io.mycat.proxy.session.AuthenticatorImpl;

import java.util.Map;

public class AuthenticatorController {
    public static void update(Map<String, UserConfig> map){
        AuthenticatorImpl authenticator = new AuthenticatorImpl(map);
        MetaClusterCurrent.register(Authenticator.class,authenticator);
    }
    public static void add( UserConfig userConfig){
        Map<String, UserConfig> config = MetaClusterCurrent.wrapper(Authenticator.class).getConfig();
        config.put(userConfig.getUsername(),userConfig);
        update(config);
    }
    public static void remove( String userName){
        Map<String, UserConfig> config = MetaClusterCurrent.wrapper(Authenticator.class).getConfig();
        config.remove(userName);
        update(config);
    }
}
