/**
 * Copyright (C) <2019>  <mycat>
 * <p>
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.beans.mysql;

/**
 * 版本信息常量
 *
 * @author wuzhihui
 *
 */
public class MySQLVersion {

    /** 协议版本 **/
    public static final byte PROTOCOL_VERSION = 10;

    /** 服务器版本 **/
    public static String SERVER_VERSION_STRING = "5.7.33-mycat-2.0";
   // public static String SERVER_VERSION_STRING = "5.6.39-mycat-2.0";

    public static byte[] SERVER_VERSION = SERVER_VERSION_STRING.getBytes();

    public static void setServerVersion(String version) {
        SERVER_VERSION = version.getBytes();
    }
}
