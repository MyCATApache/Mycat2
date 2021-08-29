package io.mycat.config.controller;

import io.mycat.Authenticator;
import io.mycat.MetaClusterCurrent;
import io.mycat.config.UserConfig;
import io.mycat.proxy.session.AuthenticatorImpl;
import jdk.nashorn.internal.runtime.arrays.ArrayLikeIterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class UserController {
   public static void  update(Map<String, UserConfig> config) {
        MetaClusterCurrent.register(MetaClusterCurrent.copyContext(Authenticator.class, new AuthenticatorImpl(config)));
    }
    public static void  add( UserConfig config) {
        Authenticator authenticator = MetaClusterCurrent.wrapper(Authenticator.class);
        HashMap<String, UserConfig> map = new HashMap<>(authenticator.getConfig());
        map.put(config.getUsername(),config);
        MetaClusterCurrent.register(MetaClusterCurrent.copyContext(Authenticator.class, new AuthenticatorImpl(map)));
    }
    public static void  remove( String name) {
        Authenticator authenticator = MetaClusterCurrent.wrapper(Authenticator.class);
        HashMap<String, UserConfig> map = new HashMap<>(authenticator.getConfig());
        map.remove(name);
        MetaClusterCurrent.register(MetaClusterCurrent.copyContext(Authenticator.class, new AuthenticatorImpl(map)));
    }
}
