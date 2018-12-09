package io.mycat.mycat2.bufferTest;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.testTool.TestUtil;
import io.mycat.mysql.ComQueryState;
import io.mycat.mysql.MySQLPacketInf;
import io.mycat.mysql.MySQLProxyPacketResolver;
import io.mycat.mysql.ServerStatus;
import io.mycat.mysql.packet.PreparedOKPacket;
import io.mycat.proxy.ProxyBuffer;
import org.junit.Assert;
import org.junit.Test;

import static io.mycat.mysql.ComQueryState.*;
import static io.mycat.mysql.MySQLPayloadType.*;


public class MySQLProxyPacketSMTest {

    @Test
    public void testComQueryFirstErr() {
        MySQLProxyPacketResolver rs = new MySQLProxyPacketResolver();
        rs.state = ComQueryState.FIRST_PACKET;
        ProxyBuffer buffer = TestUtil.errBuffer();
        MySQLPacketInf inf = new MySQLPacketInf(buffer);
        inf.head = 0xff;
        rs.resolvePayloadType(inf, true);
        Assert.assertEquals(ERROR, rs.mysqlPacketType);
    }

    @Test
    public void testComQueryFirstOk() {
        MySQLProxyPacketResolver rs = new MySQLProxyPacketResolver();
        rs.state = ComQueryState.FIRST_PACKET;
        ProxyBuffer buffer = TestUtil.ok(ServerStatus.AUTO_COMMIT);
        MySQLPacketInf inf = new MySQLPacketInf(buffer);
        inf.head = 0x00;
        rs.resolvePayloadType(inf, true);
        Assert.assertEquals(OK, rs.mysqlPacketType);

        Assert.assertEquals(ServerStatus.AUTO_COMMIT, rs.serverStatus);
    }

    @Test
    public void testComQueryFirstEof() {
        MySQLProxyPacketResolver rs = new MySQLProxyPacketResolver();
        rs.state = ComQueryState.FIRST_PACKET;
        ProxyBuffer buffer = TestUtil.eof(ServerStatus.AUTO_COMMIT);
        MySQLPacketInf inf = new MySQLPacketInf(buffer);
        inf.head = 0xFE;
        rs.resolvePayloadType(inf, true);
        Assert.assertEquals(EOF, rs.mysqlPacketType);
        Assert.assertEquals(ServerStatus.AUTO_COMMIT, rs.serverStatus);
    }

    @Test
    public void testComQueryFirstColumnCount() {
        MySQLProxyPacketResolver rs = new MySQLProxyPacketResolver();
        rs.state = ComQueryState.FIRST_PACKET;
        ProxyBuffer buffer = TestUtil.fieldCount(2);
        MySQLPacketInf inf = new MySQLPacketInf(buffer);
        inf.head = 1;
        rs.resolvePayloadType(inf, true);
        Assert.assertEquals(COLUMN_COUNT, rs.mysqlPacketType);
        Assert.assertEquals(2, rs.columnCount);
    }

    @Test
    public void testComQueryColumnDefWithoutEof() {
        MySQLProxyPacketResolver rs = new MySQLProxyPacketResolver(MySQLSession.getClientCapabilityFlags(), true);
        rs.columnCount = 1;
        rs.state = COLUMN_DEFINITION;
        ProxyBuffer buffer = TestUtil.exampleBuffer();
        MySQLPacketInf inf = new MySQLPacketInf(buffer);
        rs.resolvePayloadType(inf, false);
        Assert.assertEquals(COULUMN_DEFINITION, rs.mysqlPacketType);
        Assert.assertEquals(RESULTSET_ROW, rs.state);
    }

    @Test
    public void testComQueryColumnDefWithEof() {
        MySQLProxyPacketResolver rs = new MySQLProxyPacketResolver(MySQLSession.getClientCapabilityFlags(), false);
        rs.columnCount = 1;
        rs.state = COLUMN_DEFINITION;
        ProxyBuffer buffer = TestUtil.exampleBuffer();
        MySQLPacketInf inf = new MySQLPacketInf(buffer);
        rs.resolvePayloadType(inf, false);
        Assert.assertEquals(COULUMN_DEFINITION, rs.mysqlPacketType);
        Assert.assertEquals(COLUMN_END_EOF, rs.state);
    }

    @Test
    public void testComQueryColumnDefEnd() {
        MySQLProxyPacketResolver rs = new MySQLProxyPacketResolver(MySQLSession.getClientCapabilityFlags(), false);
        rs.state = COLUMN_END_EOF;
        ProxyBuffer buffer = TestUtil.eof(ServerStatus.AUTO_COMMIT);
        MySQLPacketInf inf = new MySQLPacketInf(buffer);
        rs.resolvePayloadType(inf, true);
        Assert.assertEquals(EOF, rs.mysqlPacketType);
        Assert.assertEquals(RESULTSET_ROW, rs.state);
    }

    @Test
    public void testComQueryTextRow() {
        MySQLProxyPacketResolver rs = new MySQLProxyPacketResolver();
        rs.state = RESULTSET_ROW;
        ProxyBuffer buffer = TestUtil.exampleBuffer();
        MySQLPacketInf inf = new MySQLPacketInf(buffer);
        inf.head = 1;
        rs.resolvePayloadType(inf, false);
        Assert.assertEquals(TEXT_RESULTSET_ROW, rs.mysqlPacketType);
    }

    @Test
    public void testComQueryBinRow() {
        MySQLProxyPacketResolver rs = new MySQLProxyPacketResolver();
        rs.state = RESULTSET_ROW;
        ProxyBuffer buffer = TestUtil.exampleBuffer();
        MySQLPacketInf inf = new MySQLPacketInf(buffer);
        inf.head = 0;
        rs.resolvePayloadType(inf, false);
        Assert.assertEquals(BINARY_RESULTSET_ROW, rs.mysqlPacketType);
    }

    @Test
    public void testComQueryRowEndWithEof() {
        MySQLProxyPacketResolver rs = new MySQLProxyPacketResolver(MySQLSession.getClientCapabilityFlags(), false);
        rs.state = RESULTSET_ROW;
        ProxyBuffer buffer = TestUtil.eof(ServerStatus.AUTO_COMMIT);
        MySQLPacketInf inf = new MySQLPacketInf(buffer);
        inf.head = 0xfe;
        rs.resolvePayloadType(inf, true);
        Assert.assertEquals(EOF, rs.mysqlPacketType);
        Assert.assertEquals(ServerStatus.AUTO_COMMIT, rs.serverStatus);
        Assert.assertEquals(RESP_END, rs.state);
    }

    @Test
    public void testComQueryRowEndWithOk() {
        MySQLProxyPacketResolver rs = new MySQLProxyPacketResolver(MySQLSession.getClientCapabilityFlags(), true);
        rs.state = RESULTSET_ROW;
        ProxyBuffer buffer = TestUtil.ok(ServerStatus.AUTO_COMMIT);
        MySQLPacketInf inf = new MySQLPacketInf(buffer);
        inf.head = 0xfe;
        rs.resolvePayloadType(inf, true);
        Assert.assertEquals(OK, rs.mysqlPacketType);
        Assert.assertEquals(ServerStatus.AUTO_COMMIT, rs.serverStatus);
        Assert.assertEquals(RESP_END, rs.state);
    }

    @Test
    public void testComQueryRowEndWithMutilResultset() {
        MySQLProxyPacketResolver rs = new MySQLProxyPacketResolver(MySQLSession.getClientCapabilityFlags(), true);
        rs.state = RESULTSET_ROW;
        ProxyBuffer buffer = TestUtil.ok(ServerStatus.MORE_RESULTS);
        MySQLPacketInf inf = new MySQLPacketInf(buffer);
        inf.head = 0xfe;
        rs.resolvePayloadType(inf, true);
        Assert.assertEquals(OK, rs.mysqlPacketType);
        Assert.assertEquals(ServerStatus.MORE_RESULTS, rs.serverStatus);
        Assert.assertEquals(FIRST_PACKET, rs.state);
    }

    @Test
    public void testComQueryRowLikeEof() {
        MySQLProxyPacketResolver rs = new MySQLProxyPacketResolver(MySQLSession.getClientCapabilityFlags(), true);
        rs.state = RESULTSET_ROW;
        ProxyBuffer buffer1 = TestUtil.exampleBuffer();
        TestUtil.anyPacket(0xffffff, 3, buffer1);
        buffer1.writeByte((byte) 0xFE);
        MySQLPacketInf inf = new MySQLPacketInf(buffer1);
        inf.head = 0xfe;
        inf.pkgLength = 0xfffffff;
        rs.resolvePayloadType(inf, true);
        Assert.assertEquals(TEXT_RESULTSET_ROW, rs.mysqlPacketType);
        Assert.assertEquals(RESULTSET_ROW, rs.state);
    }

    @Test
    public void testPrepareOK() {
        MySQLProxyPacketResolver rs = new MySQLProxyPacketResolver();
        rs.state = ComQueryState.FIRST_PACKET;
        ProxyBuffer buffer1 = TestUtil.exampleBuffer();
        PreparedOKPacket preparedOKPacket = new PreparedOKPacket();
        preparedOKPacket.packetId = 1;
        preparedOKPacket.filler = 0;
        preparedOKPacket.columnsNumber = 1;
        preparedOKPacket.parametersNumber = 2;
        preparedOKPacket.write(buffer1);
        MySQLPacketInf inf = new MySQLPacketInf(buffer1);
        inf.head = 0x00;
        inf.pkgLength = 16;
        rs.sqlType = MySQLCommand.COM_STMT_PREPARE;
        rs.resolvePayloadType(inf, true);
        Assert.assertEquals(PREPARE_OK, rs.mysqlPacketType);
        Assert.assertEquals(PREPARE_FIELD, rs.state);
        Assert.assertEquals(1,rs.prepareFieldNum);
        Assert.assertEquals(2,rs.prepareParamNum);
    }

    @Test
    public void testPrepareOKRespField1Param2WithEof() {
        MySQLProxyPacketResolver rs = new MySQLProxyPacketResolver();
        ProxyBuffer buffer1 = TestUtil.exampleBuffer();
        TestUtil.anyPacket(12, 0, buffer1);
        MySQLPacketInf inf = new MySQLPacketInf(buffer1);
        rs.prepareFieldNum = 1;
        rs.prepareParamNum = 2;
        rs.state = PREPARE_FIELD;
        inf.head = 0x00;
        inf.pkgLength = 16;
        rs.sqlType = MySQLCommand.COM_STMT_PREPARE;
        rs.resolvePayloadType(inf, true);
        Assert.assertEquals(COULUMN_DEFINITION, rs.mysqlPacketType);
        inf.head = 0xFE;
        rs.resolvePayloadType(inf, true);
        Assert.assertEquals(EOF, rs.mysqlPacketType);
        rs.resolvePayloadType(inf, true);
        Assert.assertEquals(COULUMN_DEFINITION, rs.mysqlPacketType);
        rs.resolvePayloadType(inf, true);
        Assert.assertEquals(COULUMN_DEFINITION, rs.mysqlPacketType);
        inf.head = 0xFE;
        rs.resolvePayloadType(inf, true);
        Assert.assertEquals(EOF, rs.mysqlPacketType);
        Assert.assertEquals(RESP_END, rs.state);
    }
    @Test
    public void testPrepareOKRespField2Param1WithEof() {
        MySQLProxyPacketResolver rs = new MySQLProxyPacketResolver();
        ProxyBuffer buffer1 = TestUtil.exampleBuffer();
        TestUtil.anyPacket(12, 0, buffer1);
        MySQLPacketInf inf = new MySQLPacketInf(buffer1);
        rs.prepareFieldNum = 2;
        rs.prepareParamNum = 1;
        rs.state = PREPARE_FIELD;
        inf.head = 0x00;
        inf.pkgLength = 16;
        rs.sqlType = MySQLCommand.COM_STMT_PREPARE;
        rs.resolvePayloadType(inf, true);
        Assert.assertEquals(COULUMN_DEFINITION, rs.mysqlPacketType);
        rs.resolvePayloadType(inf, true);
        Assert.assertEquals(COULUMN_DEFINITION, rs.mysqlPacketType);
        inf.head = 0xFE;
        rs.resolvePayloadType(inf, true);
        Assert.assertEquals(EOF, rs.mysqlPacketType);
        rs.resolvePayloadType(inf, true);
        Assert.assertEquals(COULUMN_DEFINITION, rs.mysqlPacketType);
        inf.head = 0xFE;
        rs.resolvePayloadType(inf, true);
        Assert.assertEquals(EOF, rs.mysqlPacketType);
        Assert.assertEquals(RESP_END, rs.state);
    }
    @Test
    public void testPrepareOKRespField1Param2WithoutEof() {
        MySQLProxyPacketResolver rs = new MySQLProxyPacketResolver(MySQLSession.getClientCapabilityFlags(),true);
        ProxyBuffer buffer1 = TestUtil.exampleBuffer();
        TestUtil.anyPacket(12, 0, buffer1);
        MySQLPacketInf inf = new MySQLPacketInf(buffer1);
        rs.prepareFieldNum = 1;
        rs.prepareParamNum = 2;
        rs.state = PREPARE_FIELD;
        inf.head = 0x00;
        inf.pkgLength = 16;
        rs.sqlType = MySQLCommand.COM_STMT_PREPARE;
        rs.resolvePayloadType(inf, true);
        Assert.assertEquals(COULUMN_DEFINITION, rs.mysqlPacketType);
        Assert.assertEquals(0, rs.prepareFieldNum);
        Assert.assertEquals(2, rs.prepareParamNum);
        rs.resolvePayloadType(inf, true);
        Assert.assertEquals(COULUMN_DEFINITION, rs.mysqlPacketType);
        rs.resolvePayloadType(inf, true);
        Assert.assertEquals(COULUMN_DEFINITION, rs.mysqlPacketType);
        Assert.assertEquals(RESP_END, rs.state);
    }
    @Test
    public void testPrepareOKRespField0Param2WithoutEof() {
        MySQLProxyPacketResolver rs = new MySQLProxyPacketResolver(MySQLSession.getClientCapabilityFlags(),true);
        ProxyBuffer buffer1 = TestUtil.exampleBuffer();
        TestUtil.anyPacket(12, 0, buffer1);
        MySQLPacketInf inf = new MySQLPacketInf(buffer1);
        rs.prepareFieldNum = 0;
        rs.prepareParamNum = 2;
        rs.state = PREPARE_PARAM;
        inf.head = 0x00;
        inf.pkgLength = 16;
        rs.sqlType = MySQLCommand.COM_STMT_PREPARE;
        rs.resolvePayloadType(inf, true);
        Assert.assertEquals(COULUMN_DEFINITION, rs.mysqlPacketType);
        rs.resolvePayloadType(inf, true);
        Assert.assertEquals(COULUMN_DEFINITION, rs.mysqlPacketType);
        Assert.assertEquals(RESP_END, rs.state);
    }
    @Test
    public void testPrepareOKRespField2Param1WithoutEof() {
        MySQLProxyPacketResolver rs = new MySQLProxyPacketResolver(MySQLSession.getClientCapabilityFlags(),true);
        ProxyBuffer buffer1 = TestUtil.exampleBuffer();
        TestUtil.anyPacket(12, 0, buffer1);
        MySQLPacketInf inf = new MySQLPacketInf(buffer1);
        rs.prepareFieldNum = 2;
        rs.prepareParamNum = 1;
        rs.state = PREPARE_FIELD;
        inf.head = 0x00;
        inf.pkgLength = 16;
        rs.sqlType = MySQLCommand.COM_STMT_PREPARE;
        rs.resolvePayloadType(inf, true);
        Assert.assertEquals(COULUMN_DEFINITION, rs.mysqlPacketType);
        rs.resolvePayloadType(inf, true);
        Assert.assertEquals(COULUMN_DEFINITION, rs.mysqlPacketType);
        rs.resolvePayloadType(inf, true);
        Assert.assertEquals(0, rs.prepareFieldNum);
        Assert.assertEquals(0, rs.prepareParamNum);
        Assert.assertEquals(COULUMN_DEFINITION, rs.mysqlPacketType);
        Assert.assertEquals(RESP_END, rs.state);
    }
    @Test
    public void testPrepareOKRespField2Param0WithoutEof() {
        MySQLProxyPacketResolver rs = new MySQLProxyPacketResolver(MySQLSession.getClientCapabilityFlags(),true);
        ProxyBuffer buffer1 = TestUtil.exampleBuffer();
        TestUtil.anyPacket(12, 0, buffer1);
        MySQLPacketInf inf = new MySQLPacketInf(buffer1);
        rs.prepareFieldNum = 2;
        rs.prepareParamNum = 0;
        rs.state = PREPARE_FIELD;
        inf.head = 0x00;
        inf.pkgLength = 16;
        rs.sqlType = MySQLCommand.COM_STMT_PREPARE;
        rs.resolvePayloadType(inf, true);
        Assert.assertEquals(COULUMN_DEFINITION, rs.mysqlPacketType);
        rs.resolvePayloadType(inf, true);
        Assert.assertEquals(COULUMN_DEFINITION, rs.mysqlPacketType);
        Assert.assertEquals(0, rs.prepareFieldNum);
        Assert.assertEquals(0, rs.prepareParamNum);
        Assert.assertEquals(RESP_END, rs.state);
    }
}
