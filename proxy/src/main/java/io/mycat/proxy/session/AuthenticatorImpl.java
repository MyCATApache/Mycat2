package io.mycat.proxy.session;

import io.mycat.Authenticator;
import io.mycat.MycatException;
import io.mycat.beans.mysql.MySQLErrorCode;
import io.mycat.config.UserConfig;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public class AuthenticatorImpl implements Authenticator {
    final Map<String, Matcher> userMatchers = new HashMap<>();
    final Map<String, UserConfig> map;
    public AuthenticatorImpl(Map<String, UserConfig> map) {
        for (Map.Entry<String,UserConfig> stringUserConfigEntry : map.entrySet()) {
         UserConfig value = stringUserConfigEntry.getValue();
            Predicate<String> stringPredicate;
            if (value.getIp() != null) {
                stringPredicate = Pattern.compile(value.getIp()).asPredicate();
            } else {
                stringPredicate = (i) -> true;
            }
            Matcher matcher = new Matcher(value.getPassword(), stringPredicate);
            userMatchers.put(stringUserConfigEntry.getKey(), matcher);
        }
        this.map = map;
    }

    @Override
    public AuthInfo getPassword(String username, String ip) {
        Matcher matcher = userMatchers.get(username);
        if (matcher == null) {
            return new AuthInfo(("user:" + username + " is not existed"),
                    null,
                    MySQLErrorCode.ER_ACCESS_DENIED_ERROR);
        }
        if( !matcher.getIpPredicate().test(ip)){
            return new AuthInfo(("ip:" + ip + " is banned"), null,
                    MySQLErrorCode.ER_ACCESS_DENIED_ERROR);
        }
        return new AuthInfo(null, matcher.getRightPassword(),
                MySQLErrorCode.ER_ACCESS_DENIED_ERROR);
    }

    @Override
    public UserConfig getUserInfo(String username) {
        return map.get(username);
    }

    @Override
    public List<UserConfig> allUsers() {
        return new ArrayList<>(map.values());
    }

    @AllArgsConstructor
    @Data
    static class Matcher {
        final String rightPassword;
        final Predicate<String> ipPredicate;
    }
}