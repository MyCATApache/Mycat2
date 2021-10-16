package io.mycat.config;

import io.mycat.util.JsonUtil;
import lombok.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@EqualsAndHashCode
public class UserConfig implements KVObject {
    private String username;
    private String password;
    private String ip = null;
    private String transactionType = "proxy";
    private String dialect = "mysql";

    public static void main(String[] args) {
        String s = JsonUtil.toJson(new UserConfig());
        System.out.println(s);
    }

    @Override
    public String keyName() {
        return username;
    }

    @Override
    public String path() {
        return "users";
    }

    @Override
    public String fileName() {
        return "user";
    }
}