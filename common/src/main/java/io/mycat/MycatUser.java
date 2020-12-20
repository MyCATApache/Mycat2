/**
 * Copyright (C) <2020>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat;

import io.mycat.config.UserConfig;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;

/**
 * @author jamie12221 date 2020-01-08 13:21
 **/
@Data
@Builder
@ToString
public class MycatUser {
    String userName;
    byte[] password;
    byte[] seed;
    String host;
    UserConfig userConfig;

    public MycatUser(String userName,
                     byte[] password,
                     byte[] seed,
                     String host,
                     UserConfig userConfig) {
        this.userName = userName;
        this.password = password;
        this.seed = seed;
        this.host = host;
        this.userConfig = userConfig;
    }
}
