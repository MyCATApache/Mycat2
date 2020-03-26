package io.mycat.proxy.session;

import lombok.AllArgsConstructor;
import lombok.Data;

public interface Authenticator {
    AuthInfo getPassword(String username, String ip);

   @Data
   @AllArgsConstructor
  public static class AuthInfo{
      Exception exception;
      String rightPassword;
     public boolean  isOk(){
         return exception == null;
     }
  }
}