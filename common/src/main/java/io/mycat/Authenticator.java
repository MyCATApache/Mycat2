package io.mycat;

import io.mycat.config.UserConfig;
import lombok.AllArgsConstructor;
import lombok.Data;

public interface Authenticator {
    AuthInfo getPassword(String username, String ip);

    UserConfig getUserInfo(String username);

    @Data
    @AllArgsConstructor
    public static class AuthInfo {
        Exception exception;
        String rightPassword;

        public boolean isOk() {
            return exception == null;
        }
    }
}