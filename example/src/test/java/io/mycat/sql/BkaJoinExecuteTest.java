package io.mycat.sql;

import io.mycat.assemble.MycatTest;
import io.mycat.drdsrunner.Explain;
import io.mycat.hint.CreateClusterHint;
import io.mycat.hint.CreateDataSourceHint;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class BkaJoinExecuteTest implements MycatTest {

    @Test
    public void testBase() throws Exception {
        initShardingTable();
        String sql;
        String explain;
        String s;
        try(Connection mycatConnection = getMySQLConnection(DB_MYCAT);){

            //first
             sql = "select * from db1.sharding s inner join db1.normal e on s.id = e.id order by s.id";
            explain= explain(mycatConnection,sql );
             s = executeQueryAsText(mycatConnection, sql);
            Assert.assertEquals(true,explain.contains("MycatSQLTableLookup"));
            Assert.assertEquals("[{id=1, user_id=null, traveldate=null, fee=null, days=null, blob=null, id0=1, companyname=Intel, addressid=1}, {id=2, user_id=null, traveldate=null, fee=null, days=null, blob=null, id0=2, companyname=IBM, addressid=2}, {id=3, user_id=null, traveldate=null, fee=null, days=null, blob=null, id0=3, companyname=Dell, addressid=3}]",
                    s);

            //second
            sql = "select * from db1.sharding s inner join db1.normal e on s.id = e.id inner join db1.global g on s.id = g.id order by s.id";
            explain= explain(mycatConnection,sql );
            s = executeQueryAsText(mycatConnection, sql);
            Assert.assertEquals(true,explain.contains("MycatSQLTableLookup"));
            Assert.assertEquals("[{id=1, user_id=null, traveldate=null, fee=null, days=null, blob=null, id0=1, companyname=Intel, addressid=1, id1=1, companyname0=Intel, addressid0=1}, {id=2, user_id=null, traveldate=null, fee=null, days=null, blob=null, id0=2, companyname=IBM, addressid=2, id1=2, companyname0=IBM, addressid0=2}, {id=3, user_id=null, traveldate=null, fee=null, days=null, blob=null, id0=3, companyname=Dell, addressid=3, id1=3, companyname0=Dell, addressid0=3}]",
                    s);

            sql = "select * from db1.normal s inner join db1.sharding e on s.id = e.id inner join db1.global g on s.id = g.id order by s.id";
            explain= explain(mycatConnection,sql );
            s = executeQueryAsText(mycatConnection, sql);
            Assert.assertEquals(true,explain.contains("MycatSQLTableLookup"));
            Assert.assertEquals("[{id=1, companyname=Intel, addressid=1, id0=1, user_id=null, traveldate=null, fee=null, days=null, blob=null, id1=1, companyname0=Intel, addressid0=1}, {id=2, companyname=IBM, addressid=2, id0=2, user_id=null, traveldate=null, fee=null, days=null, blob=null, id1=2, companyname0=IBM, addressid0=2}, {id=3, companyname=Dell, addressid=3, id0=3, user_id=null, traveldate=null, fee=null, days=null, blob=null, id1=3, companyname0=Dell, addressid0=3}]",
                    s);

            sql = "/*+MYCAT:use_values_join(s,e) use_values_join(s,g)*/select * from db1.sharding s left join db1.normal e on s.id = e.id inner join db1.global g on s.id = g.id  order by s.id";
            explain= explain(mycatConnection,sql );
            Assert.assertEquals(true,explain.contains("VALUES"));
            System.out.println();

            sql = "select * from db1.sharding s inner join db1.normal e on s.id = e.id and s.user_id = e.companyname  order by s.id";
            explain= explain(mycatConnection,sql );
            List<Map<String, Object>> maps = executeQuery(mycatConnection, sql);
            Assert.assertEquals(true,
                    explain.contains("((`normal`.`id`, `normal`.`companyname`) IN"));
            System.out.println();
        }

    }

    private void initShardingTable() throws Exception {
        Connection mycatConnection = getMySQLConnection(DB_MYCAT);
        execute(mycatConnection, RESET_CONFIG);

        execute(mycatConnection, "DROP DATABASE db1");


        execute(mycatConnection, "CREATE DATABASE db1");

        execute(mycatConnection, CreateDataSourceHint
                .create("ds0",
                        DB1));
        execute(mycatConnection, CreateDataSourceHint
                .create("ds1",
                        DB2));

        execute(mycatConnection,
                CreateClusterHint.create("c0",
                        Arrays.asList("ds0"), Collections.emptyList()));
        execute(mycatConnection,
                CreateClusterHint.create("c1",
                        Arrays.asList("ds1"), Collections.emptyList()));

        execute(mycatConnection, "USE `db1`;");

        execute(mycatConnection, "CREATE TABLE db1.`sharding` (\n" +
                "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                "  `user_id` varchar(100) DEFAULT NULL,\n" +
                "  `traveldate` date DEFAULT NULL,\n" +
                "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                "  `days` int DEFAULT NULL,\n" +
                "  `blob` longblob,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  KEY `id` (`id`)\n" +
                ") ENGINE=InnoDB  DEFAULT CHARSET=utf8"
                + " dbpartition by mod_hash(id) tbpartition by mod_hash(id) tbpartitions 2 dbpartitions 2;");
        execute(mycatConnection, "CREATE TABLE `normal` ( `id` int(11) NOT NULL AUTO_INCREMENT,`companyname` varchar(20) DEFAULT NULL,`addressid` int(11) DEFAULT NULL,PRIMARY KEY (`id`))");
        execute(mycatConnection, "CREATE TABLE `global` ( `id` int(11) NOT NULL AUTO_INCREMENT,`companyname` varchar(20) DEFAULT NULL,`addressid` int(11) DEFAULT NULL,PRIMARY KEY (`id`)) broadcast");

        execute(mycatConnection, "delete from db1.sharding");
        execute(mycatConnection, "delete from `db1`.`normal`");
        execute(mycatConnection, "delete from `db1`.`global`");
        for (int i = 1; i < 10; i++) {
            execute(mycatConnection, "insert db1.sharding (id) values(" + i + ")");
        }
        long count = count(mycatConnection, "db1", "sharding");
        Assert.assertEquals(9,count);

        execute(mycatConnection, "INSERT INTO `db1`.`normal` (id,`companyname`,`addressid`) VALUES (1,'Intel',1)");
        execute(mycatConnection, "INSERT INTO `db1`.`normal` (id,`companyname`,`addressid`) VALUES (2,'IBM',2)");
        execute(mycatConnection, "INSERT INTO `db1`.`normal` (id,`companyname`,`addressid`) VALUES (3,'Dell',3)");

        execute(mycatConnection, "INSERT INTO `db1`.`global` (id,`companyname`,`addressid`) VALUES (1,'Intel',1)");
        execute(mycatConnection, "INSERT INTO `db1`.`global` (id,`companyname`,`addressid`) VALUES (2,'IBM',2)");
        execute(mycatConnection, "INSERT INTO `db1`.`global` (id,`companyname`,`addressid`) VALUES (3,'Dell',3)");

        mycatConnection.close();
    }
}
