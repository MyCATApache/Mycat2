package io.mycat.mycat2.e2e;

import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.MysqlXAConnection;
import com.mysql.cj.jdbc.MysqlXid;
import org.junit.Assert;
import org.junit.Test;

import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * SQLCOM_XA_START
 * SQLCOM_XA_END
 * SQLCOM_XA_PREPARE
 * SQLCOM_XA_COMMIT
 * SQLCOM_XA_ROLLBACK
 * SQLCOM_XA_RECOVER
 * @author : zhuqiang
 * @version : V1.0
 * @date : 2018/11/7 22:33
 */
public class XATransactionsSQLTest extends BaseSQLTest {
    /*
     <pte>
     # 事务开始
     XA START 'xatest';
     INSERT INTO `db1`.`travelrecord` (`id`, `user_id`, `traveldate`, `fee`, `days`) VALUES ('10', '9', NULL, '10', '0');
     INSERT INTO `db1`.`travelrecord` (`id`, `user_id`, `traveldate`, `fee`, `days`) VALUES ('11', '9', NULL, '10', '0');


     XA END 'xatest';

     XA PREPARE 'xatest';

     XA ROLLBACK 'xatest';

     XA COMMIT 'xatest';

     XA RECOVER;
     </pte>
     */

    /** 执行成功，再清理插入的数据 */
    @Test
    public void xa1() {
        XATransactionsSQLTest xa = new XATransactionsSQLTest();
        xa.xaSuccess();
        xa.clear();
    }

    @Test
    public void xaSuccess() {
        byte[] gtrid = "g12345".getBytes();
        int formatId = 1;
        byte[] bqual1 = "b00001".getBytes();
        byte[] bqual2 = "b00002".getBytes();
        Xid xid1 = new MysqlXid(gtrid, bqual1, formatId);
        Xid xid2 = new MysqlXid(gtrid, bqual2, formatId);

        XAResource rm1 = null;
        XAResource rm2 = null;
        try {
            Connection c1 = newConnection();
            Connection c2 = newConnection();
            XAConnection xa1 = new MysqlXAConnection((JdbcConnection) c1, true);
            XAConnection xa2 = new MysqlXAConnection((JdbcConnection) c2, true);
            rm1 = xa1.getXAResource();
            rm2 = xa2.getXAResource();

            rm1.start(xid1, XAResource.TMNOFLAGS);
            PreparedStatement p1 = c1.prepareStatement("INSERT INTO travelrecord (`id`, `user_id`, `traveldate`, `fee`, `days`)" +
                    " VALUES ('10', '9', NULL, '10', '0');");
            p1.execute();
            rm1.end(xid1, XAResource.TMSUCCESS);

            rm2.start(xid2, XAResource.TMNOFLAGS);
            PreparedStatement p2 = c2.prepareStatement("INSERT INTO `db1`.`travelrecord` (`id`, `user_id`, `traveldate`, `fee`, `days`)" +
                    " VALUES ('11', '9', NULL, '10', '0');");
            p2.execute();
            rm2.end(xid2, XAResource.TMSUCCESS);

            // 预备
            int prepare1 = rm1.prepare(xid1);
            int prepare2 = rm2.prepare(xid2);

            if (prepare1 == XAResource.XA_OK && prepare2 == XAResource.XA_OK) {
                rm1.commit(xid1, false); // 两阶段提交
                rm2.commit(xid2, false); // 两阶段提交
            } else {
                // 有失败的则都回滚
                rm1.rollback(xid1);
                rm2.rollback(xid2);
            }
        } catch (Exception e) {
            try {
                if (rm1 != null) {
                    rm1.rollback(xid1);
                }
                if (rm2 != null) {
                    rm2.rollback(xid2);
                }
            } catch (XAException e1) {
                e1.printStackTrace();
            }
        }
    }

    @Test
    public void clear() {
        using(c -> {
            c.createStatement().execute("delete from travelrecord where id in (10,11)");
        });
    }

    /** 执行后，回滚 */
    @Test
    public void xaRollback() throws SQLException, XAException {
        byte[] gtrid = "g12345".getBytes();
        int formatId = 1;
        byte[] bqual1 = "b00001".getBytes();
        byte[] bqual2 = "b00002".getBytes();
        Xid xid1 = new MysqlXid(gtrid, bqual1, formatId);
        Xid xid2 = new MysqlXid(gtrid, bqual2, formatId);

        XAResource rm1 = null;
        XAResource rm2 = null;

        Connection c1 = newConnection();
        Connection c2 = newConnection();
        XAConnection xa1 = new MysqlXAConnection((JdbcConnection) c1, true);
        XAConnection xa2 = new MysqlXAConnection((JdbcConnection) c2, true);
        rm1 = xa1.getXAResource();
        rm2 = xa2.getXAResource();

        rm1.start(xid1, XAResource.TMNOFLAGS);
        PreparedStatement p1 = c1.prepareStatement("INSERT INTO travelrecord (`id`, `user_id`, `traveldate`, `fee`, `days`)" +
                " VALUES ('10', '9', NULL, '10', '0');");
        p1.execute();
        rm1.end(xid1, XAResource.TMSUCCESS);

        rm2.start(xid2, XAResource.TMNOFLAGS);
        PreparedStatement p2 = c2.prepareStatement("INSERT INTO `db1`.`travelrecord` (`id`, `user_id`, `traveldate`, `fee`, `days`)" +
                " VALUES ('11', '9', NULL, '10', '0');");
        p2.execute();
        rm2.end(xid2, XAResource.TMSUCCESS);

        // 预备
        int prepare1 = rm1.prepare(xid1);
        int prepare2 = rm2.prepare(xid2);

        rm1.rollback(xid1);
        rm2.rollback(xid2);
    }

    @Test
    public void xaRecover() throws SQLException, XAException {
        byte[] gtrid = "g12345".getBytes();
        int formatId = 1;
        byte[] bqual1 = "b00001".getBytes();
        Xid xid1 = new MysqlXid(gtrid, bqual1, formatId);
        Connection c1 = newConnection();
        XAConnection xa1 = new MysqlXAConnection((JdbcConnection) c1, true);
        XAResource rm1 = xa1.getXAResource();
        rm1.start(xid1, XAResource.TMNOFLAGS);
        PreparedStatement p1 = c1.prepareStatement("INSERT INTO travelrecord (`id`, `user_id`, `traveldate`, `fee`, `days`)" +
                " VALUES ('10', '9', NULL, '10', '0');");
        p1.execute();
        rm1.end(xid1, XAResource.TMSUCCESS);
        int prepare1 = rm1.prepare(xid1);

        Xid[] recover = rm1.recover(XAResource.TMSTARTRSCAN);
        Xid xid = recover[0];
        String x = new String(xid.getGlobalTransactionId()) + new String(xid.getBranchQualifier());
        System.out.println(x);
        Assert.assertEquals("g12345b00001", x);
        rm1.rollback(xid1);
    }
}
