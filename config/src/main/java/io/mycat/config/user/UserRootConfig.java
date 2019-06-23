/**
 * Copyright (C) <2019>  <gaozhiwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.mycat.config.user;

import io.mycat.config.ConfigurableRoot;
import java.util.List;

/**
 * Desc: 对应user.yml文件
 *
 * date: 19/09/2017
 * @author: gaozhiwen
 */
public class UserRootConfig extends ConfigurableRoot {
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
