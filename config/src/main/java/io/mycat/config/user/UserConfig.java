package io.mycat.config.user;

import java.util.List;

/**
 * Desc: user配置类
 *
 * @date: 24/09/2017
 * @author: gaozhiwen
 */
public class UserConfig {
    private String name;
    private String password;
    private List<String> schemas;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public List<String> getSchemas() {
        return schemas;
    }

    public void setSchemas(List<String> schemas) {
        this.schemas = schemas;
    }

    @Override
    public String toString() {
        return "UserConfig{" + "name='" + name + '\'' + ", password='" + password + '\'' + ", schemas=" + schemas + '}';
    }
}
