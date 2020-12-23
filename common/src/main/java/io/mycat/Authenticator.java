package io.mycat;

import io.mycat.config.UserConfig;
import lombok.Data;

import java.util.List;

public interface Authenticator {

    AuthInfo getPassword(String username, String ip);

    UserConfig getUserInfo(String username);

    List<UserConfig> allUsers();

    @Data
    public static class AuthInfo {
        String exception;
        String rightPassword;
        int errorCode;

        public AuthInfo(String exception, String rightPassword, int errorCode) {
            this.exception = exception;
            this.rightPassword = rightPassword;
            this.errorCode = errorCode;
        }

        public boolean isOk() {
            return exception == null;
        }
    }
}