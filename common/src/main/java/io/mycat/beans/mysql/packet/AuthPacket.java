/**
 * Copyright (C) <2019>  <zhu qiang>
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
package io.mycat.beans.mysql.packet;


import io.mycat.config.MySQLServerCapabilityFlags;

import java.util.HashMap;
import java.util.Map;

/**
 * HandshakeResponse41
 * https://dev.mysql.com/doc/internals/en/connection-phase-packets.html
 *
 * <pre>
 *      4              capability flags, CLIENT_PROTOCOL_41 always set
 *      4              max-packet size
 *      1              character set
 *      string[23]     reserved (all [0])
 *      string[NUL]    username
 *      if getCapabilities &amp; CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA {   // 理解auth响应数据的长度编码整数
 *          lenenc-int     length of auth-response
 *          string[n]      auth-response
 *      } else if getCapabilities &amp; CLIENT_SECURE_CONNECTION {  // 支持 Authentication::Native41。
 *          1              length of auth-response
 *          string[n]      auth-response
 *      } else {
 *          string[NUL]    auth-response
 *      }
 *      if getCapabilities &amp; CLIENT_CONNECT_WITH_DB {  // 可以在 connect 中指定数据库（模式）名称
 *          string[NUL]    database
 *      }
 *      if getCapabilities &amp; CLIENT_PLUGIN_AUTH {  // 在初始握手包中发送额外数据， 并支持可插拔认证协议。
 *          string[NUL]    auth plugin name
 *      }
 *      if getCapabilities &amp; CLIENT_CONNECT_ATTRS {  // 允许连接属性
 *          lenenc-int     length of all key-values
 *          lenenc-str     key
 *          lenenc-str     value
 *         if-more sql in 'length of all key-values', more keys and value pairs
 *      }
 *
 *  关于字符集：https://dev.mysql.com/doc/refman/8.0/en/charset-metadata.html
 *  大部分都是 utf8,且在连接前都是 utf8.几乎上不用设置编码
 * </pre>
 *
 * @author : zhuqiang
 *  date : 2018/11/14 21:40
 *
/**
 * @author jamie12221
 *  date 2019-05-07 13:58
 *
 * 验证包
 **/
public class AuthPacket {

    private static final byte[] RESERVED = new byte[23];
    private int capabilities;
    private int maxPacketSize;
    private byte characterSet;
    private String username;
    private byte[] password;
    private String database;
    private String authPluginName;
    private Map<String, String> clientConnectAttrs;

    /**
     * 计算 LengthEncodedInteger 的字节长度
     *
     * @param val
     * @return
     */
    public static int calcLenencLength(int val) {
        if (val < 251) {
            return 1;
        } else if (val >= 251 && val < (1 << 16)) {
            return 3;
        } else if (val >= (1 << 16) && val < (1 << 24)) {
            return 4;
        } else {
            return 9;
        }
    }

    public static byte[] getRESERVED() {
        return RESERVED;
    }

    public void readPayload(MySQLPayloadReadView buffer) {
        capabilities = (int) buffer.readFixInt(4);
        maxPacketSize = (int) buffer.readFixInt(4);
        characterSet = buffer.readByte();
        buffer.readBytes(RESERVED.length);
        username = buffer.readNULString();
        if (MySQLServerCapabilityFlags.isPluginAuthLenencClientData(capabilities)) {
            password = buffer.readFixStringBytes(buffer.readLenencInt());
        } else if ((MySQLServerCapabilityFlags.isCanDo41Anthentication(capabilities))) {
            int passwordLength = buffer.readByte();
            password = buffer.readFixStringBytes(passwordLength);
        } else {
            password = buffer.readNULStringBytes();
        }

        if (MySQLServerCapabilityFlags.isConnectionWithDatabase(capabilities)) {
            database = buffer.readNULString();
        }

        if (MySQLServerCapabilityFlags.isPluginAuth(capabilities)) {
            authPluginName = buffer.readNULString();
        }

        if (MySQLServerCapabilityFlags.isConnectAttrs(capabilities) && !buffer.readFinished()) {
            long kvAllLength = buffer.readLenencInt();
            if (kvAllLength != 0) {
                clientConnectAttrs = new HashMap<>();
            }
            int count = 0;
            while (count < kvAllLength) {
                String k = buffer.readLenencString();
                String v = buffer.readLenencString();
                count += k.length();
                count += v.length();
                count += calcLenencLength(k.length());
                count += calcLenencLength(v.length());
                clientConnectAttrs.put(k, v);
            }
        }
    }

    public void writePayload(MySQLPayloadWriteView buffer) {
        buffer.writeFixInt(4, capabilities);
        buffer.writeFixInt(4, maxPacketSize);
        buffer.writeByte(characterSet);
        buffer.writeBytes(RESERVED);
        buffer.writeNULString(username);
        if (MySQLServerCapabilityFlags.isPluginAuthLenencClientData(capabilities)) {
            buffer.writeLenencInt(password.length);
            buffer.writeFixString(password);
        } else if (MySQLServerCapabilityFlags.isCanDo41Anthentication(capabilities)) {
            buffer.writeFixInt(1, password.length);
            buffer.writeFixString(password);
        } else {
            buffer.writeNULString(password);
        }

        if (MySQLServerCapabilityFlags.isConnectionWithDatabase(capabilities)
                && database != null) {
            buffer.writeNULString(database);
        }

        if ((MySQLServerCapabilityFlags.isPluginAuth(capabilities)
                && authPluginName != null)) {
            buffer.writeNULString(authPluginName);
        }

        if (MySQLServerCapabilityFlags.isConnectAttrs(capabilities)
                && clientConnectAttrs != null && !clientConnectAttrs.isEmpty()) {
            int kvAllLength = 0;
            for (Map.Entry<String, String> item : clientConnectAttrs.entrySet()) {
                kvAllLength += item.getKey().length();
                kvAllLength += item.getValue().length();
            }
            buffer.writeLenencInt(kvAllLength);
            clientConnectAttrs.forEach((k, v) -> buffer.writeLenencString(k).writeLenencString(v));
        }
    }

    public int getCapabilities() {
        return capabilities;
    }

    public void setCapabilities(int capabilities) {
        this.capabilities = capabilities;
    }

    public int getMaxPacketSize() {
        return maxPacketSize;
    }

    public void setMaxPacketSize(int maxPacketSize) {
        this.maxPacketSize = maxPacketSize;
    }

    public int getCharacterSet() {
        return characterSet & 0xff;
    }

    public void setCharacterSet(byte characterSet) {
        this.characterSet = characterSet;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public byte[] getPassword() {
        return password;
    }

    public void setPassword(byte[] password) {
        this.password = password;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getAuthPluginName() {
        return authPluginName;
    }

    public void setAuthPluginName(String authPluginName) {
        this.authPluginName = authPluginName;
    }

    public Map<String, String> getClientConnectAttrs() {
        return clientConnectAttrs;
    }

    public void setClientConnectAttrs(Map<String, String> clientConnectAttrs) {
        this.clientConnectAttrs = clientConnectAttrs;
    }
}
