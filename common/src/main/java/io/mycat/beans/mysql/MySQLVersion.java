/**
 * Copyright (C) <2019>  <mycat>
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
	public static byte[] SERVER_VERSION = "8.0.11-mycat-2.0".getBytes();

	public static void setServerVersion(String version) {
		byte[] mysqlVersionPart = version.getBytes();
		int startIndex;
		for (startIndex = 0; startIndex < SERVER_VERSION.length; startIndex++) {
			if (SERVER_VERSION[startIndex] == '-')
				break;
		}

		// 重新拼接mycat version字节数组
		byte[] newMycatVersion = new byte[mysqlVersionPart.length + SERVER_VERSION.length - startIndex];
		System.arraycopy(mysqlVersionPart, 0, newMycatVersion, 0, mysqlVersionPart.length);
		System.arraycopy(SERVER_VERSION, startIndex, newMycatVersion, mysqlVersionPart.length,
				SERVER_VERSION.length - startIndex);
		SERVER_VERSION = newMycatVersion;
	}
}
