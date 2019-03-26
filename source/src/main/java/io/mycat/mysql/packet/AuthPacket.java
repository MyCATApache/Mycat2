package io.mycat.mysql.packet;


import io.mycat.mysql.Capabilities;
import io.mycat.proxy.ProxyBuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;
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
 *      if capabilities & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA {   // 理解auth响应数据的长度编码整数
 *          lenenc-int     length of auth-response
 *          string[n]      auth-response
 *      } else if capabilities & CLIENT_SECURE_CONNECTION {  // 支持 Authentication::Native41。
 *          1              length of auth-response
 *          string[n]      auth-response
 *      } else {
 *          string[NUL]    auth-response
 *      }
 *      if capabilities & CLIENT_CONNECT_WITH_DB {  // 可以在 connect 中指定数据库（模式）名称
 *          string[NUL]    database
 *      }
 *      if capabilities & CLIENT_PLUGIN_AUTH {  // 在初始握手包中发送额外数据， 并支持可插拔认证协议。
 *          string[NUL]    auth plugin name
 *      }
 *      if capabilities & CLIENT_CONNECT_ATTRS {  // 允许连接属性
 *          lenenc-int     length of all key-values
 *          lenenc-str     key
 *          lenenc-str     value
 *         if-more sql in 'length of all key-values', more keys and value pairs
 *      }
 *
 *  关于字符集：https://dev.mysql.com/doc/refman/8.0/en/charset-metadata.html
 *  大部分都是 utf8,且在连接前都是 utf8.几乎上不用设置编码
 * </pre>
 * @author : zhuqiang
 * @date : 2018/11/14 21:40
 */
public class AuthPacket {
    public byte packetId;
    public int capabilities;
    public int maxPacketSize;
    public byte characterSet;
    public static final byte[] RESERVED = new byte[23];
    public String username;
    public byte[] password;
    public String database;
    public String authPluginName;
    public Map<String, String> clientConnectAttrs;

    public void read(ProxyBuffer buffer) {
        int packetLength = (int) buffer.readFixInt(3);
        packetId = buffer.readByte();
        readPayload(buffer);
    }

    public void readPayload(ProxyBuffer buffer) {
        capabilities = (int) buffer.readFixInt(4);
        maxPacketSize = (int) buffer.readFixInt(4);
        characterSet = buffer.readByte();
        buffer.readBytes(RESERVED.length);
        username = buffer.readNULString();
        if ((capabilities & Capabilities.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)
                == Capabilities.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) {
            password = buffer.readFixStringBytes((int) buffer.readLenencInt());
        } else if ((capabilities & Capabilities.CLIENT_SECURE_CONNECTION)
                == Capabilities.CLIENT_SECURE_CONNECTION) {
            int passwordLength = buffer.readByte();
            password = buffer.readFixStringBytes(passwordLength);
        } else {
            password = buffer.readNULStringBytes();
        }

        if ((capabilities & Capabilities.CLIENT_CONNECT_WITH_DB) == Capabilities.CLIENT_CONNECT_WITH_DB) {
            database = buffer.readNULString();
        }

        if ((capabilities & Capabilities.CLIENT_PLUGIN_AUTH) == Capabilities.CLIENT_PLUGIN_AUTH) {
            authPluginName = buffer.readNULString();
        }

        if ((capabilities & Capabilities.CLIENT_CONNECT_ATTRS) == Capabilities.CLIENT_CONNECT_ATTRS) {
            long kvAllLength = buffer.readLenencInt();
            if (kvAllLength != 0) {
                clientConnectAttrs = new HashMap<>();
            }
            int count = 0;
            while (count < kvAllLength) {
//                byte[] k = buffer.readLenencStringBytes();
//                byte[] v = buffer.readLenencStringBytes();
//                count += k.length;
//                count += v.length;
//                count += calcLenencLength(k.length);
//                count += calcLenencLength(v.length);
//                clientConnectAttrs.put(new String(k), new String(v));
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

    public void write(ProxyBuffer buffer) {
        buffer.writeFixInt(3, 0);
        buffer.writeByte(packetId);
        writePayload(buffer);
        ByteBuffer b = buffer.getBuffer();
        int position = b.position();
        b.put(0, (byte) (position - 4));
    }

    public void writePayload(ProxyBuffer buffer) {
        buffer.writeFixInt(4, capabilities);
        buffer.writeFixInt(4, maxPacketSize);
        buffer.writeByte(characterSet);
        buffer.writeBytes(RESERVED);
        buffer.writeNULString(username);
        if ((capabilities & Capabilities.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)
                == Capabilities.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) {
            buffer.writeLenencInt(password.length);
            buffer.writeFixString(password);
        } else if ((capabilities & Capabilities.CLIENT_SECURE_CONNECTION)
                == Capabilities.CLIENT_SECURE_CONNECTION) {
            buffer.writeFixInt(1, password.length);
            buffer.writeFixString(password);
        } else {
            buffer.writeNULString(password);
        }

        if ((capabilities & Capabilities.CLIENT_CONNECT_WITH_DB) == Capabilities.CLIENT_CONNECT_WITH_DB
                && database != null) {
            buffer.writeNULString(database);
        }

        if ((capabilities & Capabilities.CLIENT_PLUGIN_AUTH) == Capabilities.CLIENT_PLUGIN_AUTH
                && authPluginName != null) {
            buffer.writeNULString(authPluginName);
        }

        if ((capabilities & Capabilities.CLIENT_CONNECT_ATTRS) == Capabilities.CLIENT_CONNECT_ATTRS
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

    /**
     * 计算 LengthEncodedInteger 的字节长度
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

    @Override
    public String toString() {
        return "AuthPacket{" +
                "packetId=" + packetId +
                ", capabilities=" + capabilities +
                ", maxPacketSize=" + maxPacketSize +
                ", characterSet=" + characterSet +
                ", username='" + username + '\'' +
                ", password=" + Arrays.toString(password) +
                ", database='" + database + '\'' +
                ", authPluginName='" + authPluginName + '\'' +
                ", clientConnectAttrs=" + clientConnectAttrs +
                '}';
    }
}
