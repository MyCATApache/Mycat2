package io.mycat.proxy.session;

import io.mycat.MycatException;
import io.mycat.config.PatternRootConfig;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public class AuthenticatorImpl implements Authenticator {
    final Map<String, Matcher> userMatchers = new HashMap<>();

    public AuthenticatorImpl(Map<String, PatternRootConfig.UserConfig> map) {
        for (Map.Entry<String, PatternRootConfig.UserConfig> stringUserConfigEntry : map.entrySet()) {
            PatternRootConfig.UserConfig value = stringUserConfigEntry.getValue();
            Predicate<String> stringPredicate;

            if (value.getIp() != null) {
                stringPredicate = Pattern.compile(value.getIp()).asPredicate();
            } else {
                stringPredicate = (i) -> true;
            }
            Matcher matcher = new Matcher(value.getPassword(), stringPredicate);
            userMatchers.put(stringUserConfigEntry.getKey(), matcher);
        }
    }

    @Override
    public AuthInfo getPassword(String username, String ip) {
        Matcher matcher = userMatchers.get(username);
        if (matcher == null) {
            return new AuthInfo(new MycatException("user:" + username + " is not existed"), null);
        }
        if( !matcher.getIpPredicate().test(ip)){
            return new AuthInfo(new MycatException("ip:" + ip + " is banned"), null);
        }
        return new AuthInfo(null, matcher.getRightPassword());
    }

    @AllArgsConstructor
    @Data
    static class Matcher {
        final String rightPassword;
        final Predicate<String> ipPredicate;
    }
}