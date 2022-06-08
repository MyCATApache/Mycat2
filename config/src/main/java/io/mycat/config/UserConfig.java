package io.mycat.config;

import com.alibaba.druid.util.StringUtils;
import io.mycat.util.Base64Utils;
import io.mycat.util.JsonUtil;
import io.mycat.util.RSAUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import lombok.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@EqualsAndHashCode
public class UserConfig implements KVObject {
    private static final Logger LOGGER = LoggerFactory.getLogger(UserConfig.class);

    private String username;
    private String password;
    private String ip = null;
    private String transactionType = "proxy";
    private String dialect = "mysql";
    private String schema;
    private int isolation = 3;
    private boolean readOnly = false;
    private int loginLimit = -1;
    private String encryptType = "";
    private String encodeKey = "";
    private Map<String, SchemaPrivilege> schemaPrivileges = new HashMap<String, SchemaPrivilege>();

    /**
     * copy from mycat1.6
     * 库级权限
     */
    public static class SchemaPrivilege {

        private String name;
        private int[] dml = new int[]{0, 0, 0, 0};

        private Map<String, TablePrivilege> tablePrivileges = new HashMap<String, TablePrivilege>();

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int[] getDml() {
            return dml;
        }

        public void setDml(int[] dml) {
            this.dml = dml;
        }

        public void addTablePrivilege(String tableName, TablePrivilege privilege) {
            this.tablePrivileges.put(tableName, privilege);
        }

        public TablePrivilege getTablePrivilege(String tableName) {
            TablePrivilege tablePrivilege = tablePrivileges.get(tableName);
            if (tablePrivilege == null) {
                tablePrivilege = new TablePrivilege();
                tablePrivilege.setName(tableName);
                tablePrivilege.setDml(dml);
            }
            return tablePrivilege;
        }
    }

    /**
     * copy from mycat1.6
     * 表级权限
     */
    public static class TablePrivilege {

        private String name;
        private int[] dml = new int[]{0, 0, 0, 0};

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int[] getDml() {
            return dml;
        }

        public void setDml(int[] dml) {
            this.dml = dml;
        }

    }

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

    public String getPassword() {
        if (StringUtils.isEmpty(encryptType)) {
            return password;
        }
        String decodePassword =
                EncryptType.getByName(encryptType).getDecryptFunction().apply(password, getEncodeKey());
        if (StringUtils.isEmpty(decodePassword)) {
            throw new RuntimeException("用户密码无法正确解密");
        }
        return decodePassword;
    }

    @Getter
    enum EncryptType {
        NONE("", (encodePassword, key) -> encodePassword),
        RSA(
                "rsa",
                (encodePassword, key) -> {
                    try {
                        return RSAUtils.decrypt(encodePassword, key);
                    } catch (Exception e) {
                        LOGGER.error(e.getMessage(), e);
                    }
                    return null;
                });

        String name;
        BiFunction<String, String, String> decryptFunction;

        EncryptType(String name, BiFunction<String, String, String> decryptFunction) {
            this.name = name;
            this.decryptFunction = decryptFunction;
        }

        public static EncryptType getByName(String name) {
            for (EncryptType value : EncryptType.values()) {
                if (value.getName().equals(name)) {
                    return value;
                }
            }
            return EncryptType.NONE;
        }
    }
}
