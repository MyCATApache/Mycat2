package io.mycat.mysql.packet;

import io.mycat.proxy.ProxyBuffer;
import io.mycat.util.SecurityUtil;
import io.mycat.util.StringUtil;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;

/**
 * ${todo}
 * @author : zhuqiang
 * @date : 2018/11/14 23:37
 */
public class NewAuthPacketTest {

    @Test
    public void writePayload() throws NoSuchAlgorithmException {
        NewAuthPacket newAuthPacket = new NewAuthPacket();
        NewHandshakePacket.CapabilityFlags capabilities = new NewHandshakePacket.CapabilityFlags(0);
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

        newAuthPacket.capabilities = capabilities.value;
        newAuthPacket.characterSet = 8;
        newAuthPacket.maxPacketSize = 16 * 1024 * 1024;
        newAuthPacket.username = "root".getBytes();
        newAuthPacket.password = passwd("123456");
        newAuthPacket.database = "db1".getBytes();
        newAuthPacket.authPluginName = "mysql_native_password".getBytes();

        ProxyBuffer buffer = new ProxyBuffer(ByteBuffer.allocate(1024));
        newAuthPacket.write(buffer);
        buffer.flip();
        System.out.println(StringUtil.dumpAsHex(buffer.getBuffer()));
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
}