package io.mycat.ui;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

@EqualsAndHashCode
@Data
@ToString
public class ConfigBroadcaster {
   LoginInfo master = new LoginInfo();
   List<LoginInfo> slaves = new ArrayList<>();
    Type type;
    public static enum Type{
       DB,
       FILE
   }

   @Data
   @EqualsAndHashCode
    public static class LoginInfo {
        String url = "jdbc:mysql://localhost:3306/mysql";
        String user = "root";
        String password = "123456";
    }
}
