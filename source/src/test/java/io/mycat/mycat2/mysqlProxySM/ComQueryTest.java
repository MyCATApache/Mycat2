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
public class ComQueryTest {

    MySQLPacketCallback callback = new MySQLPacketCallback() {
    };
    @Test
    public void test_ok() {
        MySQLProxyStateM sm = new MySQLProxyStateM(callback);
        sm.in(MySQLCommand.COM_QUERY);
        sm.on(MySQLPacket.OK_PACKET, ServerStatus.builder().build());
        Assert.assertTrue(sm.isFinished());
        Assert.assertFalse(sm.isInteractive());
    }
    @Test
    public void test_err() {
        MySQLProxyStateM sm = new MySQLProxyStateM(callback);
        sm.in(MySQLCommand.COM_QUERY);
        sm.on(MySQLPacket.ERROR_PACKET, ServerStatus.builder().build());
        Assert.assertTrue(sm.isFinished());
        Assert.assertFalse(sm.isInteractive());
    }

    @Test
    public void test_one_field_eof_empry_row_eof() {
        MySQLProxyStateM sm = new MySQLProxyStateM(callback);
        sm.in(MySQLCommand.COM_QUERY);
        sm.on(NOT_OK_EOF_ERR);//fieldCount
        sm.on(NOT_OK_EOF_ERR);//field
        sm.on(MySQLPacket.EOF_PACKET);
        sm.on(MySQLPacket.EOF_PACKET);//empty row
        Assert.assertTrue(sm.isFinished());
        Assert.assertFalse(sm.isInteractive());
    }
    @Test
    public void test_empty_field() {
        MySQLProxyStateM sm = new MySQLProxyStateM(callback);
        sm.in(MySQLCommand.COM_QUERY);
        sm.on(MySQLPacket.OK_PACKET, ServerStatus.builder().build());
        Assert.assertTrue(sm.isFinished());
        Assert.assertFalse(sm.isInteractive());
    }
    @Test
    public void test_two_field_eof_empry_row_eof() {
        MySQLProxyStateM sm = new MySQLProxyStateM(callback);
        sm.in(MySQLCommand.COM_QUERY);
        sm.on(NOT_OK_EOF_ERR);//fieldCount
        sm.on(NOT_OK_EOF_ERR);//field
        sm.on(NOT_OK_EOF_ERR);//field
        sm.on(MySQLPacket.EOF_PACKET);
        sm.on(MySQLPacket.EOF_PACKET);//empty row
        Assert.assertTrue(sm.isFinished());
        Assert.assertFalse(sm.isInteractive());
    }
    @Test
    public void test_one_field_eof_one_row_eof() {
        MySQLProxyStateM sm = new MySQLProxyStateM(callback);
        sm.in(MySQLCommand.COM_QUERY);
        sm.on(NOT_OK_EOF_ERR);//fieldCount
        sm.on(NOT_OK_EOF_ERR);//field
        sm.on(MySQLPacket.EOF_PACKET);
        sm.on(NOT_OK_EOF_ERR);//one row
        sm.on(MySQLPacket.EOF_PACKET);
        Assert.assertTrue(sm.isFinished());
        Assert.assertFalse(sm.isInteractive());
    }

    @Test
    public void test_one_field_eof_one_row_err() {
        MySQLProxyStateM sm = new MySQLProxyStateM(callback);
        sm.in(MySQLCommand.COM_QUERY);
        sm.on(NOT_OK_EOF_ERR);//fieldCount
        sm.on(NOT_OK_EOF_ERR);//field
        sm.on(MySQLPacket.EOF_PACKET);
        sm.on(NOT_OK_EOF_ERR);//one row
        sm.on(MySQLPacket.ERROR_PACKET);
        Assert.assertTrue(sm.isFinished());
        Assert.assertFalse(sm.isInteractive());
    }
    @Test
    public void test_one_field_eof_two_row_eof() {
        MySQLProxyStateM sm = new MySQLProxyStateM(callback);
        sm.in(MySQLCommand.COM_QUERY);
        sm.on(NOT_OK_EOF_ERR);//fieldCount
        sm.on(NOT_OK_EOF_ERR);//field
        sm.on(MySQLPacket.EOF_PACKET);
        sm.on(NOT_OK_EOF_ERR);//two row
        sm.on(MySQLPacket.EOF_PACKET);
        Assert.assertTrue(sm.isFinished());
        Assert.assertFalse(sm.isInteractive());
    }

    @Test
    public void test_load_data_ok() {
        MySQLProxyStateM sm = new MySQLProxyStateM(callback);
        sm.in(MySQLCommand.COM_QUERY);
        //interactive with client
        sm.on(MySQLPacket.OK_PACKET);
        Assert.assertTrue(sm.isFinished());
        Assert.assertFalse(sm.isInteractive());
    }
    @Test
    public void test_load_data_error() {
        MySQLProxyStateM sm = new MySQLProxyStateM(callback);
        sm.in(MySQLCommand.COM_QUERY);
        //interactive with client
        sm.on(MySQLPacket.ERROR_PACKET);
        Assert.assertTrue(sm.isFinished());
        Assert.assertFalse(sm.isInteractive());
    }


    @Test
    public void test_more_result() {
        MySQLProxyStateM sm = new MySQLProxyStateM(callback);
        sm.in(MySQLCommand.COM_QUERY);

        sm.on(NOT_OK_EOF_ERR);//fieldCount
        sm.on(NOT_OK_EOF_ERR);//field
        sm.on(MySQLPacket.EOF_PACKET);
        sm.on(NOT_OK_EOF_ERR);//two row
        sm.on(MySQLPacket.EOF_PACKET,ServerStatus.builder().setMoreResult().build());
        Assert.assertFalse(sm.isFinished());
        Assert.assertTrue(sm.isInteractive());

        sm.on(NOT_OK_EOF_ERR,ServerStatus.builder().build());//fieldCount
        sm.on(NOT_OK_EOF_ERR);//field
        sm.on(MySQLPacket.EOF_PACKET);
        sm.on(NOT_OK_EOF_ERR);//two row

        sm.on(MySQLPacket.EOF_PACKET,ServerStatus.builder().build());
        Assert.assertTrue(sm.isFinished());
        Assert.assertFalse(sm.isInteractive());
    }

    @Test
    public void test_multi_resultset() {
        MySQLProxyStateM sm = new MySQLProxyStateM(callback);
        sm.in(MySQLCommand.COM_QUERY);

        sm.on(NOT_OK_EOF_ERR);//fieldCount
        sm.on(NOT_OK_EOF_ERR);//field
        sm.on(MySQLPacket.EOF_PACKET);
        sm.on(NOT_OK_EOF_ERR);//two row
        sm.on(MySQLPacket.EOF_PACKET,ServerStatus.builder().setMoreResult().build());
        Assert.assertFalse(sm.isFinished());
        Assert.assertTrue(sm.isInteractive());

        sm.on(NOT_OK_EOF_ERR);//fieldCount
        sm.on(NOT_OK_EOF_ERR);//field
        sm.on(MySQLPacket.EOF_PACKET);
        sm.on(NOT_OK_EOF_ERR);//two row

        sm.on(MySQLPacket.EOF_PACKET,ServerStatus.builder().setMoreResult().build());

        /**
         * As of MySQL 5.7.5, the resultset is followed by an OK_Packet, and this
         * OK_Packet has the SERVER_MORE_RESULTS_EXISTS flag set to start
         * processing the next resultset.
         */
        sm.on(MySQLPacket.OK_PACKET,ServerStatus.builder().setMulitQuery().build());
        Assert.assertFalse(sm.isFinished());
        Assert.assertTrue(sm.isInteractive());

        sm.on(MySQLPacket.OK_PACKET,ServerStatus.builder().build());
        Assert.assertTrue(sm.isFinished());
        Assert.assertFalse(sm.isInteractive());
    }

    @Test
    public void test_multi_resultset2() {
        MySQLProxyStateM sm = new MySQLProxyStateM(callback);
        sm.in(MySQLCommand.COM_QUERY);

        sm.on(MySQLPacket.OK_PACKET,ServerStatus.builder().setMulitQuery().build());
        Assert.assertFalse(sm.isFinished());
        Assert.assertTrue(sm.isInteractive());

        sm.on(MySQLPacket.OK_PACKET,ServerStatus.builder().build());
        Assert.assertTrue(sm.isFinished());
        Assert.assertFalse(sm.isInteractive());
    }
    @Test
    public void test_multi_resultset3() {
        MySQLProxyStateM sm = new MySQLProxyStateM(callback);
        sm.in(MySQLCommand.COM_QUERY);

        sm.on(MySQLPacket.OK_PACKET,ServerStatus.builder().setMulitQuery().build());
        Assert.assertFalse(sm.isFinished());
        Assert.assertTrue(sm.isInteractive());

        sm.on(NOT_OK_EOF_ERR);//fieldCount
        sm.on(NOT_OK_EOF_ERR);//field
        sm.on(MySQLPacket.EOF_PACKET);
        sm.on(NOT_OK_EOF_ERR);//two row
        sm.on(MySQLPacket.EOF_PACKET,ServerStatus.builder().setMoreResult().build());
        Assert.assertFalse(sm.isFinished());
        Assert.assertTrue(sm.isInteractive());


        sm.on(MySQLPacket.OK_PACKET,ServerStatus.builder().build());
        Assert.assertTrue(sm.isFinished());
        Assert.assertFalse(sm.isInteractive());
    }

}
