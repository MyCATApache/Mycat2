package io.mycat.mycat2.mysqlProxySM;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.cmds.judge.MySQLPacketCallback;
import io.mycat.mycat2.cmds.judge.MySQLProxyStateM;
import io.mycat.mysql.ServerStatus;
import io.mycat.mysql.packet.MySQLPacket;
import org.junit.Assert;
import org.junit.Test;

import static io.mycat.mysql.packet.MySQLPacket.NOT_OK_EOF_ERR;

/**
 * cjw
 * 294712221@qq.com
 */
public class ComSTMTTest {
    MySQLPacketCallback callback = new MySQLPacketCallback() {
    };
    @Test
    public void test_prepare() {
        MySQLProxyStateM sm = new MySQLProxyStateM(callback);
        sm.in(MySQLCommand.COM_STMT_PREPARE);
        sm.on(MySQLPacket.OK_PACKET, ServerStatus.builder().build(),true);
        sm.prepareFieldNum = 0;
        sm.prepareParamNum = 0;
        Assert.assertFalse(sm.isFinished());
        Assert.assertTrue(sm.isInteractive());
    }
    @Test
    public void test_prepare2() {
        MySQLProxyStateM sm = new MySQLProxyStateM(callback);
        sm.in(MySQLCommand.COM_STMT_PREPARE);
        sm.on(MySQLPacket.OK_PACKET, ServerStatus.builder().build());
        sm.prepareFieldNum = 0;
        sm.prepareParamNum = 0;
        Assert.assertFalse(sm.isFinished());
        Assert.assertTrue(sm.isInteractive());
    }

    @Test
    public void test_prepare_1_0() {
        MySQLProxyStateM sm = new MySQLProxyStateM(callback);
        sm.in(MySQLCommand.COM_STMT_PREPARE);
        sm.on(MySQLPacket.OK_PACKET, ServerStatus.builder().build());
        sm.prepareFieldNum = 1;
        sm.prepareParamNum = 0;
        sm.on(MySQLPacket.NOT_OK_EOF_ERR);
        sm.on(MySQLPacket.EOF_PACKET);
        Assert.assertTrue(sm.isFinished());
        Assert.assertFalse(sm.isInteractive());
    }
    @Test
    public void test_prepare_0_1() {
        MySQLProxyStateM sm = new MySQLProxyStateM(callback);
        sm.in(MySQLCommand.COM_STMT_PREPARE);
        sm.on(MySQLPacket.OK_PACKET, ServerStatus.builder().build());
        sm.prepareFieldNum = 0;
        sm.prepareParamNum = 1;
        sm.on(MySQLPacket.NOT_OK_EOF_ERR);
        sm.on(MySQLPacket.EOF_PACKET);
        Assert.assertTrue(sm.isFinished());
        Assert.assertFalse(sm.isInteractive());
    }
    @Test
    public void test_prepare_1_1() {
        MySQLProxyStateM sm = new MySQLProxyStateM(callback);
        sm.in(MySQLCommand.COM_STMT_PREPARE);
        sm.on(MySQLPacket.OK_PACKET, ServerStatus.builder().build());
        sm.prepareFieldNum = 0;
        sm.prepareParamNum = 1;
        sm.on(MySQLPacket.NOT_OK_EOF_ERR);
        sm.on(MySQLPacket.EOF_PACKET);
        sm.on(MySQLPacket.NOT_OK_EOF_ERR);
        sm.on(MySQLPacket.EOF_PACKET);
        Assert.assertTrue(sm.isFinished());
        Assert.assertFalse(sm.isInteractive());
    }
    @Test
    public void test_prepare_2_1() {
        MySQLProxyStateM sm = new MySQLProxyStateM(callback);
        sm.in(MySQLCommand.COM_STMT_PREPARE);
        sm.on(MySQLPacket.OK_PACKET, ServerStatus.builder().build());
        sm.prepareFieldNum = 0;
        sm.prepareParamNum = 1;
        sm.on(MySQLPacket.NOT_OK_EOF_ERR);
        sm.on(MySQLPacket.NOT_OK_EOF_ERR);
        sm.on(MySQLPacket.EOF_PACKET);
        sm.on(MySQLPacket.NOT_OK_EOF_ERR);
        sm.on(MySQLPacket.EOF_PACKET);
        Assert.assertTrue(sm.isFinished());
        Assert.assertFalse(sm.isInteractive());
    }
    @Test
    public void test_prepare_1_2() {
        MySQLProxyStateM sm = new MySQLProxyStateM(callback);
        sm.in(MySQLCommand.COM_STMT_PREPARE);
        sm.on(MySQLPacket.OK_PACKET, ServerStatus.builder().build());
        sm.prepareFieldNum = 0;
        sm.prepareParamNum = 1;
        sm.on(MySQLPacket.NOT_OK_EOF_ERR);
        sm.on(MySQLPacket.EOF_PACKET);
        sm.on(MySQLPacket.NOT_OK_EOF_ERR);
        sm.on(MySQLPacket.NOT_OK_EOF_ERR);
        sm.on(MySQLPacket.EOF_PACKET);
        Assert.assertTrue(sm.isFinished());
        Assert.assertFalse(sm.isInteractive());
    }
    @Test
    public void test_execute_ok() {
        MySQLProxyStateM sm = new MySQLProxyStateM(callback);
        sm.in(MySQLCommand.COM_STMT_EXECUTE);
        sm.on(MySQLPacket.OK_PACKET, ServerStatus.builder().build());
        Assert.assertTrue(sm.isFinished());
        Assert.assertFalse(sm.isInteractive());
    }
    @Test
    public void test_execute_err() {
        MySQLProxyStateM sm = new MySQLProxyStateM(callback);
        sm.in(MySQLCommand.COM_STMT_EXECUTE);
        sm.on(MySQLPacket.ERROR_PACKET);
        Assert.assertTrue(sm.isFinished());
        Assert.assertFalse(sm.isInteractive());
    }

    @Test
    public void test_execute_resultSet() {
        MySQLProxyStateM sm = new MySQLProxyStateM(callback);
        sm.in(MySQLCommand.COM_STMT_EXECUTE);
        sm.on(MySQLPacket.NOT_OK_EOF_ERR);//binary protocol result
        sm.on(MySQLPacket.OK_PACKET, ServerStatus.builder().build());
        Assert.assertTrue(sm.isFinished());
    }

    /**
     * If the CLIENT_DEPRECATE_EOF client capability flag is set, OK_Packet
     * is sent; else EOF_Packet is sent.
     */
    @Test
    public void test_execute_resultSet2() {
        MySQLProxyStateM sm = new MySQLProxyStateM(callback);
        sm.in(MySQLCommand.COM_STMT_EXECUTE);
        sm.on(MySQLPacket.NOT_OK_EOF_ERR);//binary protocol result
        sm.on(MySQLPacket.EOF_PACKET, ServerStatus.builder().build());
        Assert.assertTrue(sm.isFinished());
    }

    @Test
    public void test_reset_ok() {
        MySQLProxyStateM sm = new MySQLProxyStateM(callback);
        sm.in(MySQLCommand.COM_STMT_RESET);
        sm.on(MySQLPacket.OK_PACKET, ServerStatus.builder().build());
        Assert.assertTrue(sm.isFinished());
    }
    @Test
    public void test_reset_err() {
        MySQLProxyStateM sm = new MySQLProxyStateM(callback);
        sm.in(MySQLCommand.COM_STMT_RESET);
        sm.on(MySQLPacket.ERROR_PACKET);
        Assert.assertTrue(sm.isFinished());
    }
    @Test
    public void test_com_set_option_eof() {
        MySQLProxyStateM sm = new MySQLProxyStateM(callback);
        sm.in(MySQLCommand.COM_SET_OPTION);
        sm.on(MySQLPacket.EOF_PACKET, ServerStatus.builder().build());
        Assert.assertTrue(sm.isFinished());
    }
    @Test
    public void test_com_set_option_err() {
        MySQLProxyStateM sm = new MySQLProxyStateM(callback);
        sm.in(MySQLCommand.COM_SET_OPTION);
        sm.on(MySQLPacket.ERROR_PACKET);
        Assert.assertTrue(sm.isFinished());
        Assert.assertFalse(sm.isInteractive());
    }
    @Test
    public void test_fetch_err() {
        MySQLProxyStateM sm = new MySQLProxyStateM(callback);
        sm.in(MySQLCommand.COM_STMT_FETCH);
        sm.on(MySQLPacket.ERROR_PACKET);
        Assert.assertTrue(sm.isFinished());
        Assert.assertTrue(sm.isInteractive());
    }
    @Test
    public void test_fetch_multi_resultset() {
        MySQLProxyStateM sm = new MySQLProxyStateM(callback);
        sm.in(MySQLCommand.COM_STMT_FETCH);

        sm.on(NOT_OK_EOF_ERR);//fieldCount
        sm.on(NOT_OK_EOF_ERR);//field
        sm.on(MySQLPacket.EOF_PACKET);
        sm.on(NOT_OK_EOF_ERR);//two row
        sm.on(MySQLPacket.EOF_PACKET,ServerStatus.builder().setMoreResult().build());
        Assert.assertFalse(sm.isFinished());
        Assert.assertTrue(sm.isInteractive());

        sm.on(NOT_OK_EOF_ERR);//fieldCount
        sm.on(NOT_OK_EOF_ERR);//field
        sm.on(MySQLPacket.EOF_PACKET,ServerStatus.builder().build());
        sm.on(NOT_OK_EOF_ERR);//two row

        sm.on(MySQLPacket.EOF_PACKET,ServerStatus.builder().build());
        Assert.assertTrue(sm.isFinished());
        Assert.assertTrue(sm.isInteractive());
    }

    /**
     * SERVER_STATUS_CURSOR_EXISTS 0x0040 Used by Binary Protocol
     * Resultset to signal that
     * COM_STMT_FETCH must be
     * used to fetch the row-data.
     */
    @Test
    public void test_stmt_cursor() {
        MySQLProxyStateM sm = new MySQLProxyStateM(callback);
        sm.in(MySQLCommand.COM_STMT_EXECUTE);//contains cursor falg
        sm.on(MySQLPacket.NOT_OK_EOF_ERR);//binary protocol result
        /**
         *
         * As of MySQL 5.7.5, the resultset is followed by an OK_Packet, and this
         * OK_Packet has the SERVER_MORE_RESULTS_EXISTS flag set to start
         * processing the next resultset
         */
        sm.on(MySQLPacket.OK_PACKET, ServerStatus.builder().setCursorExists().build());
        Assert.assertTrue(sm.isFinished());
        Assert.assertTrue(sm.isInteractive());
        sm.in(MySQLCommand.COM_STMT_FETCH);
        sm.on(MySQLPacket.OK_PACKET, ServerStatus.builder().setInTransaction().setMoreResult().build());
        sm.on(MySQLPacket.OK_PACKET, ServerStatus.builder().setInTransaction().setMoreResult().build());
        sm.on(MySQLPacket.EOF_PACKET, ServerStatus.builder().setCursorExists().build());
        Assert.assertTrue(sm.isFinished());
        Assert.assertTrue(sm.isInteractive());
        sm.in(MySQLCommand.COM_STMT_FETCH);
        sm.on(MySQLPacket.OK_PACKET, ServerStatus.builder().setInTransaction().build());
        sm.on(MySQLPacket.OK_PACKET, ServerStatus.builder().setMoreResult().build());
        sm.on(MySQLPacket.EOF_PACKET, ServerStatus.builder().setLastRowSent().build());
        Assert.assertTrue(sm.isFinished());
        Assert.assertTrue(sm.isInteractive());
        sm.in(MySQLCommand.COM_STMT_CLOSE);
        Assert.assertTrue(sm.isFinished());
        Assert.assertFalse(sm.isInteractive());
    }
}
