/**
 * Copyright (C) <2019>  <chen junwen>
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
package io.mycat.util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.NoSuchAlgorithmException;

/**
 * mysql 插件工具类
 * @author : zhuqiang
 *  date : 2018/11/25 20:03
 */
public class MysqlNativePasswordPluginUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlNativePasswordPluginUtil.class);
    public static final String PROTOCOL_PLUGIN_NAME = "mysql_native_password";

    public static byte[] scramble411(String password, byte[] seed) {
        if (password == null || password.length() == 0) {
            LOGGER.warn("password is empty");
            return new byte[0];
        }
        try {
            return SecurityUtil.scramble411(password.getBytes(), seed);
        } catch (NoSuchAlgorithmException e) {
            LOGGER.warn("no such algorithm", e);
            return null;
        }
    }

    public static byte[] scramble411(String password, String seed) {
        return scramble411(password, seed.getBytes());
    }

    public static byte[][] nextSeedBuild() {
        byte[] authPluginDataPartOne = RandomUtil.randomBytes(8);
        byte[] authPluginDataPartTwo = RandomUtil.randomBytes(12);

        // 保存认证数据
        byte[] seed = new byte[20];
        System.arraycopy(authPluginDataPartOne, 0, seed, 0, 8);
        System.arraycopy(authPluginDataPartTwo, 0, seed, 8, 12);

        byte[][] result = new byte[3][1];
        result[0] = authPluginDataPartOne;
        result[1] = authPluginDataPartTwo;
        result[2] = seed;
        return result;
    }

    public static String[] nextSeedStringBuild() {
        byte[][] bytes = nextSeedBuild();
        return new String[]{
                new String(bytes[0]),
                new String(bytes[1]),
                new String(bytes[2]),
        };
    }
}
