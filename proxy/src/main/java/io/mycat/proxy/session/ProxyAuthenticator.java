package io.mycat.proxy.session;

import io.mycat.Authenticator;
import io.mycat.MetaClusterCurrent;
import io.mycat.config.UserConfig;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ProxyAuthenticator implements Authenticator {
    @Override
    public AuthInfo getPassword(String username, String ip) {
        Authenticator authenticator = Objects.requireNonNull(MetaClusterCurrent.wrapper(Authenticator.class));
        return authenticator.getPassword(username, ip);
    }

    @Override
    public UserConfig getUserInfo(String username) {
        Authenticator authenticator = Objects.requireNonNull(MetaClusterCurrent.wrapper(Authenticator.class));
        return authenticator.getUserInfo(username);
    }

    @Override
    public List<UserConfig> getConfigAsList() {
        Authenticator authenticator = Objects.requireNonNull(MetaClusterCurrent.wrapper(Authenticator.class));
        return authenticator.getConfigAsList();
    }

    @Override
    public Map<String, UserConfig> getConfigAsMap() {
        Authenticator authenticator = Objects.requireNonNull(MetaClusterCurrent.wrapper(Authenticator.class));
        return authenticator.getConfigAsMap();
    }
}