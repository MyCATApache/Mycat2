package io.mycat.config.user;

import io.mycat.config.Configurable;

import java.util.List;

/**
 * Desc: 对应user.yml文件
 *
 * @date: 19/09/2017
 * @author: gaozhiwen
 */
public class UserRootConfig implements Configurable {
    private List<UserConfig> users;
    private FireWallConfig firewall;

    public List<UserConfig> getUsers() {
        return users;
    }

    public void setUsers(List<UserConfig> users) {
        this.users = users;
    }

    public FireWallConfig getFirewall() {
        return firewall;
    }

    public void setFirewall(FireWallConfig firewall) {
        this.firewall = firewall;
    }
}
