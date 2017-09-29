package io.mycat.mycat2.beans.conf;

import io.mycat.proxy.Configurable;

import java.util.List;

/**
 * Desc: 对应user.yml文件
 *
 * @date: 19/09/2017
 * @author: gaozhiwen
 */
public class UserConfig implements Configurable {
    private List<UserBean> users;
    private FireWallBean firewall;

    public List<UserBean> getUsers() {
        return users;
    }

    public void setUsers(List<UserBean> users) {
        this.users = users;
    }

    public FireWallBean getFirewall() {
        return firewall;
    }

    public void setFirewall(FireWallBean firewall) {
        this.firewall = firewall;
    }
}
