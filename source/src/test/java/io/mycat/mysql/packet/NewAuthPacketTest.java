package io.mycat.mysql.packet;

import io.mycat.mysql.CapabilityFlags;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.util.SecurityUtil;
import io.mycat.util.StringUtil;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 验证包测试
 * @author : zhuqiang
 * @date : 2018/11/14 23:37
 */
public class NewAuthPacketTest {

    @Test
    public void writePayload() throws NoSuchAlgorithmException {
        AuthPacket newAuthPacket = new AuthPacket();
        CapabilityFlags capabilities = new CapabilityFlags(0);
        capabilities.setLongPassword();
        capabilities.setFoundRows();
        capabilities.setLongColumnWithFLags();
        capabilities.setConnectionWithDatabase();
        capabilities.setDoNotAllowDatabaseDotTableDotColumn();
        capabilities.setCanUseCompressionProtocol();
        capabilities.setIgnoreSigpipes();
        capabilities.setODBCClient();
        capabilities.setCanUseLoadDataLocal();
        capabilities.setIgnoreSpacesBeforeLeftBracket();
        capabilities.setClientProtocol41();
        capabilities.setInteractive();
        capabilities.setSwitchToSSLAfterHandshake();
        capabilities.setIgnoreSigpipes();
        capabilities.setKnowsAboutTransactions();
        capabilities.setSpeak41Protocol();
        capabilities.setCanDo41Anthentication();
        capabilities.setPluginAuth();
        capabilities.setConnectAttrs();

        newAuthPacket.capabilities = capabilities.value;
        newAuthPacket.characterSet = 8;
        newAuthPacket.maxPacketSize = 16 * 1024 * 1024;
        newAuthPacket.username = "root";
        newAuthPacket.password = passwd("123456");
        newAuthPacket.database = "db1";
        newAuthPacket.authPluginName = "mysql_native_password";
        HashMap<String, String> clientConnectAttrs = new HashMap<>();
        newAuthPacket.clientConnectAttrs = clientConnectAttrs;
        clientConnectAttrs.put("useUnicode", "true");
        clientConnectAttrs.put("characterEncoding", "UTF-8");
        clientConnectAttrs.put("allowMultiQueries", "true");

        ProxyBuffer buffer = new ProxyBuffer(ByteBuffer.allocate(1024));
        newAuthPacket.write(buffer);
        buffer.flip();
        ByteBuffer b = buffer.getBuffer();
        int position = b.position();
        byte[] buildBytes = new byte[position];
        b.position(0);
        b.get(buildBytes);
        System.out.println(StringUtil.dumpAsHex(b));

        read(buildBytes, newAuthPacket);
    }

    private void read(byte[] buildBytes, AuthPacket originAuthPacket) {
        AuthPacket newAuthPacket = new AuthPacket();
        ProxyBuffer buffer = new ProxyBuffer(ByteBuffer.wrap(buildBytes));
        buffer.writeIndex = buildBytes.length;
        newAuthPacket.read(buffer);
        Assert.assertEquals(newAuthPacket.packetId, originAuthPacket.packetId);
        Assert.assertEquals(newAuthPacket.capabilities, originAuthPacket.capabilities);
        Assert.assertEquals(newAuthPacket.maxPacketSize, originAuthPacket.maxPacketSize);
        Assert.assertEquals(newAuthPacket.characterSet, originAuthPacket.characterSet);
        Assert.assertEquals(newAuthPacket.RESERVED, originAuthPacket.RESERVED);
        Assert.assertEquals(newAuthPacket.username, originAuthPacket.username);
        Assert.assertArrayEquals(newAuthPacket.password, originAuthPacket.password);
        Assert.assertEquals(newAuthPacket.database, originAuthPacket.database);
        Assert.assertEquals(newAuthPacket.authPluginName, originAuthPacket.authPluginName);
        Map<String, String> clientConnectAttrs = originAuthPacket.clientConnectAttrs;
        newAuthPacket.clientConnectAttrs.forEach((k, v) -> Assert.assertTrue(clientConnectAttrs.containsKey(k)));
    }

    private static byte[] passwd(String pass) throws NoSuchAlgorithmException {
        if (pass == null || pass.length() == 0) {
            return null;
        }
        byte[] passwd = pass.getBytes();
        byte[] seed1 = "mycat123".getBytes(); // 8
        byte[] seed2 = "mycatmycat12".getBytes(); // 12
        byte[] seed = new byte[20];
        System.arraycopy(seed, 0, seed1, 0, seed1.length);
        System.arraycopy(seed, seed1.length, seed2, 0, seed2.length);
        return SecurityUtil.scramble411(passwd, seed);
    }

    /*
        5.1.34 的驱动 jdbc 抓包，抓包的尾部多了一串 00.这个暂时不知道是否正常
        0000   02 00 00 00 45 00 01 19 66 62 40 00 80 06 00 00   ....E...fb@.....
        0010   7f 00 00 01 7f 00 00 01 85 d4 0c ea 98 56 91 bc   .........Ô.ê.V.¼
        0020   6b 7b 18 2c 50 18 08 04 b9 a4 00 -> ed 00 00 01   k{.,P...¹¤..í...
        0030   8f a2 3b 00 ff ff ff 00 21 00 00 00 00 00 00 00   .¢;.ÿÿÿ.!.......
        0040   00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00   ................
        0050   72 6f 6f 74 00 14 f1 b7 b5 e4 d5 1e 6b 15 1a c0   root..ñ·µäÕ.k..À
        0060   ed 01 c3 40 bd 8e a0 3b a8 42 64 62 31 00 6d 79   í.Ã@½. ;¨Bdb1.my
        0070   73 71 6c 5f 6e 61 74 69 76 65 5f 70 61 73 73 77   sql_native_passw
        0080   6f 72 64 00 8a 10 5f 72 75 6e 74 69 6d 65 5f 76   ord..._runtime_v
        0090   65 72 73 69 6f 6e 08 31 2e 38 2e 30 5f 34 35 0f   ersion.1.8.0_45.
        00a0   5f 63 6c 69 65 6e 74 5f 76 65 72 73 69 6f 6e 06   _client_version.
        00b0   35 2e 31 2e 33 34 0c 5f 63 6c 69 65 6e 74 5f 6e   5.1.34._client_n
        00c0   61 6d 65 14 4d 79 53 51 4c 20 43 6f 6e 6e 65 63   ame.MySQL Connec
        00d0   74 6f 72 20 4a 61 76 61 0f 5f 63 6c 69 65 6e 74   tor Java._client
        00e0   5f 6c 69 63 65 6e 73 65 03 47 50 4c 0f 5f 72 75   _license.GPL._ru
        00f0   6e 74 69 6d 65 5f 76 65 6e 64 6f 72 12 4f 72 61   ntime_vendor.Ora
        0100   63 6c 65 20 43 6f 72 70 6f 72 61 74 69 6f 6e <-   cle Corporation.
        0110   00 00 00 00 00 00 00 00 00 00 00 00 00            .............

        Packet Length: 237
        Packet Number: 1
        Client Capabilities: 0xa28f
        Extended Client Capabilities: 0x003b
        MAX Packet: 16777215
        Charset: utf8 COLLATE utf8_general_ci (33)
        Username: root
        Password: f1b7b5e4d51e6b151ac0ed01c340bd8ea03ba842 (123456)
        Schema: db1
        Client Auth Plugin: mysql_native_password
        Connection Attributes length: 138
        Connection Attribute - _runtime_version: 1.8.0_45
        Connection Attribute - _client_version: 5.1.34
        Connection Attribute - _client_name: MySQL Connector Java
        Connection Attribute - _client_license: GPL
        Connection Attribute - _runtime_vendor: Oracle Corporation
        Payload: 0000000000000000000000000000
     */
    @Test
    public void readFromWireshark() {
        byte[] bytes = build();
        ProxyBuffer buffer = new ProxyBuffer(ByteBuffer.wrap(bytes));
        buffer.writeIndex = bytes.length;

        AuthPacket newAuthPacket = new AuthPacket();
        newAuthPacket.read(buffer);
        Assert.assertEquals(1, newAuthPacket.packetId);
        Assert.assertEquals(0x003b, newAuthPacket.capabilities >> 16);
        Assert.assertEquals(16777215, newAuthPacket.maxPacketSize);
        Assert.assertEquals(33, newAuthPacket.characterSet);
        Assert.assertEquals("root", newAuthPacket.username);
        // 要使用随机数解密才能对比了
//        Assert.assertArrayEquals("f1b7b5e4d51e6b151ac0ed01c340bd8ea03ba842".getBytes(), newAuthPacket.password);
        Assert.assertEquals("db1", newAuthPacket.database);
        Assert.assertEquals("mysql_native_password", newAuthPacket.authPluginName);
        Map<String, String> clientConnectAttrs = newAuthPacket.clientConnectAttrs;
        Assert.assertTrue(clientConnectAttrs.containsKey("_runtime_version"));
        Assert.assertEquals("1.8.0_45", clientConnectAttrs.get("_runtime_version"));
        Assert.assertTrue(clientConnectAttrs.containsKey("_client_version"));
        Assert.assertEquals("5.1.34", clientConnectAttrs.get("_client_version"));
        Assert.assertTrue(clientConnectAttrs.containsKey("_client_name"));
        Assert.assertEquals("MySQL Connector Java", clientConnectAttrs.get("_client_name"));
        Assert.assertTrue(clientConnectAttrs.containsKey("_client_license"));
        Assert.assertEquals("GPL", clientConnectAttrs.get("_client_license"));
        Assert.assertTrue(clientConnectAttrs.containsKey("_runtime_vendor"));
        Assert.assertEquals("Oracle Corporation", clientConnectAttrs.get("_runtime_vendor"));
    }

    public byte[] build() {
        List<String> list = new ArrayList<>();
        list.add("ed 00 00 01");
        list.add("8f a2 3b 00 ff ff ff 00 21 00 00 00 00 00 00 00");
        list.add("00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00");
        list.add("72 6f 6f 74 00 14 f1 b7 b5 e4 d5 1e 6b 15 1a c0");
        list.add("ed 01 c3 40 bd 8e a0 3b a8 42 64 62 31 00 6d 79");
        list.add("73 71 6c 5f 6e 61 74 69 76 65 5f 70 61 73 73 77");
        list.add("6f 72 64 00 8a 10 5f 72 75 6e 74 69 6d 65 5f 76");
        list.add("65 72 73 69 6f 6e 08 31 2e 38 2e 30 5f 34 35 0f");
        list.add("5f 63 6c 69 65 6e 74 5f 76 65 72 73 69 6f 6e 06");
        list.add("35 2e 31 2e 33 34 0c 5f 63 6c 69 65 6e 74 5f 6e");
        list.add("61 6d 65 14 4d 79 53 51 4c 20 43 6f 6e 6e 65 63");
        list.add("74 6f 72 20 4a 61 76 61 0f 5f 63 6c 69 65 6e 74");
        list.add("5f 6c 69 63 65 6e 73 65 03 47 50 4c 0f 5f 72 75");
        list.add("6e 74 69 6d 65 5f 76 65 6e 64 6f 72 12 4f 72 61");
        list.add("63 6c 65 20 43 6f 72 70 6f 72 61 74 69 6f 6e");
        List<Byte> collect = list.stream().map(i -> i.split(" "))
                .flatMap(i -> Arrays.stream(i))
                .map(i -> (byte) Integer.parseInt(i, 16))
                .collect(Collectors.toList());

        byte[] bytes = new byte[collect.size()];
        for (int i = 0; i < collect.size(); i++) {
            bytes[i] = collect.get(i);
        }
        return bytes;
    }
}