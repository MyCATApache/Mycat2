package io.mycat.mycat2.mysql;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.TestUtil;
import io.mycat.mycat2.cmds.judge.MySQLPacketCallback;
import io.mycat.mycat2.cmds.judge.MySQLProxyStateM;
import io.mycat.mysql.ServerStatus;
import io.mycat.mysql.packet.*;
import io.mycat.proxy.ProxyBuffer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

import static io.mycat.mycat2.TestUtil.fieldCount;

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
        sm.reset(MySQLCommand.COM_QUERY);
        sm.on(MySQLPacket.OK_PACKET, ServerStatus.builder().build());
        Assert.assertTrue(sm.isFinished());
        Assert.assertFalse(sm.isInteractive());
    }
    @Test
    public void test_err() {
        MySQLProxyStateM sm = new MySQLProxyStateM(callback);
        sm.reset(MySQLCommand.COM_QUERY);
        sm.on(MySQLPacket.ERROR_PACKET, ServerStatus.builder().build());
        Assert.assertTrue(sm.isFinished());
        Assert.assertFalse(sm.isInteractive());
    }

//    @Test
//    public void test_one_field_eof_row_eof() {
//        MySQLProxyStateM sm = new MySQLProxyStateM(callback);
//        sm.reset(MySQLCommand.COM_QUERY);
//        sm.on(fieldCount());
//        Assert.assertTrue(sm.isFinished());
//        Assert.assertFalse(sm.isInteractive());
//    }

}
