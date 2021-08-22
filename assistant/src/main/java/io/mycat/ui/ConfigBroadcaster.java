package io.mycat.ui;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@EqualsAndHashCode
@Data
public class ConfigBroadcaster {
   LoginInfo master;
   List<LoginInfo> slaves = new ArrayList<>();
    Type type;
    public static enum Type{
       DB,
       FILE
   }

   @Data
   @EqualsAndHashCode
    public static class LoginInfo {
        String url;
        String user;
        String password;
    }
}
