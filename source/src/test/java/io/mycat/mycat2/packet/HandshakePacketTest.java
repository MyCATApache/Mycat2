package io.mycat.mycat2.packet;

import io.mycat.mysql.packet.NewHandshakePacket;
import io.mycat.proxy.ProxyBuffer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static io.mycat.mycat2.TestUtil.ofBuffer;
import static io.mycat.mycat2.TestUtil.of;
/**
 * cjw 294712221@qq.com
 */
public class HandshakePacketTest {

    @Test
    public void testReadHandshakePacket() {
        NewHandshakePacket handshakePacket = new NewHandshakePacket();
        handshakePacket.readPayload(ofBuffer(pkt17));
        Assert.assertEquals(0x0a, handshakePacket.protocolVersion);
        Assert.assertEquals("8.0.12", handshakePacket.serverVersion);
        Assert.assertEquals(13, handshakePacket.connectionId);
        Assert.assertEquals(new String(of(0x15, 0x5f, 0x3e, 0x56, 0x1d, 0x15, 0x2b, 0x79)), handshakePacket.authPluginDataPartOne);
        Assert.assertEquals(0xffff, handshakePacket.capabilities.getLower2Bytes());
        Assert.assertEquals(255, handshakePacket.characterSet);
        Assert.assertEquals(0x0002, handshakePacket.statusFlags);
        Assert.assertEquals(0xc3ff, handshakePacket.capabilities.getUpper2Bytes());
        Assert.assertEquals(21, handshakePacket.authPluginDataLen);
        Assert.assertEquals("^@y\b\u000F\u001CH~cLI}\u0000", handshakePacket.authPluginDataPartTwo);
        Assert.assertEquals("mysql_native_password", handshakePacket.authPluginName);
    }

    @Test
    public void testWriteHandshakePacket() {
        NewHandshakePacket handshakePacket = new NewHandshakePacket();
        handshakePacket.protocolVersion = 0x0a;
        handshakePacket.serverVersion = "8.0.12";
        handshakePacket.connectionId = 13;
        handshakePacket.authPluginDataPartOne = new String(of(0x15, 0x5f, 0x3e, 0x56, 0x1d, 0x15, 0x2b, 0x79));
        handshakePacket.hasPartTwo = true;
        handshakePacket.capabilities = new NewHandshakePacket.CapabilityFlags(0);

        handshakePacket.capabilities.setLongPassword();
        handshakePacket.capabilities.setFoundRows();
        handshakePacket.capabilities.setLongColumnWithFLags();
        handshakePacket.capabilities.setConnectionWithDatabase();
        handshakePacket.capabilities.setDoNotAllowDatabaseDotTableDotColumn();
        handshakePacket.capabilities.setCanUseCompressionProtocol();
        handshakePacket.capabilities.setIgnoreSigpipes();
        handshakePacket.capabilities.setODBCClient();
        handshakePacket.capabilities.setCanUseLoadDataLocal();
        handshakePacket.capabilities.setIgnoreSpacesBeforeLeftBracket();
        handshakePacket.capabilities.setClientProtocol41();
        handshakePacket.capabilities.setInteractive();
        handshakePacket.capabilities.setSwitchToSSLAfterHandshake();
        handshakePacket.capabilities.setIgnoreSigpipes();
        handshakePacket.capabilities.setKnowsAboutTransactions();
        handshakePacket.capabilities.setSpeak41Protocol();
        handshakePacket.capabilities.setCanDo41Anthentication();

        handshakePacket.characterSet = 255;
        handshakePacket.statusFlags = 0x0002;

        handshakePacket.capabilities.setMultipleStatements();
        handshakePacket.capabilities.setMultipleResults();
        handshakePacket.capabilities.setPSMultipleResults();

        handshakePacket.capabilities.setPluginAuth();
        handshakePacket.capabilities.setConnectAttrs();
        handshakePacket.capabilities.setPluginAuthLenencClientData();
        handshakePacket.capabilities.setClientCanHandleExpiredPasswords();
        handshakePacket.capabilities.setSessionVariableTracking();
        handshakePacket.capabilities.setDeprecateEOF();
        handshakePacket.authPluginDataLen = 21;
        handshakePacket.authPluginDataPartTwo = "^@y\b\u000F\u001CH~cLI}\u0000";
        handshakePacket.authPluginName = "mysql_native_password";

        NewHandshakePacket fact = new NewHandshakePacket();
        fact.readPayload(ofBuffer(pkt17));

        Assert.assertEquals(handshakePacket.toString(), fact.toString());
        ProxyBuffer buffer = new ProxyBuffer(ByteBuffer.allocate(pkt17.length));
        handshakePacket.writePayload(buffer);
        byte[] array = buffer.getBuffer().array();
        int[] ints = Arrays.copyOf(pkt17, pkt17.length);
        ints[27] = 1;//unused
        Assert.assertArrayEquals(of(ints),array);
        Assert.assertEquals(handshakePacket.calcPacketSize(), pkt17.length);
    }

    static int[] pkt17 = {
            0x0a, 0x38, 0x2e, 0x30, /* J....8.0 */
            0x2e, 0x31, 0x32, 0x00, 0x0d, 0x00, 0x00, 0x00, /* .12..... */
            0x15, 0x5f, 0x3e, 0x56, 0x1d, 0x15, 0x2b, 0x79, /* ._>V..+y */
            0x00, 0xff, 0xff, 0xff, 0x02, 0x00, 0xff, 0xc3, /* ........ */
            0x15, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* ........ */
            0x00, 0x00, 0x00, 0x5e, 0x40, 0x79, 0x08, 0x0f, /* ...^@y.. */
            0x1c, 0x48, 0x7e, 0x63, 0x4c, 0x49, 0x7d, 0x00, /* .H~cLI}. */
            0x6d, 0x79, 0x73, 0x71, 0x6c, 0x5f, 0x6e, 0x61, /* mysql_na */
            0x74, 0x69, 0x76, 0x65, 0x5f, 0x70, 0x61, 0x73, /* tive_pas */
            0x73, 0x77, 0x6f, 0x72, 0x64, 0x00              /* sword. */
    };


}
