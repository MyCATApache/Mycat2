package io.mycat.assemble;

import io.mycat.hint.CreateClusterHint;
import io.mycat.hint.CreateDataSourceHint;
import io.mycat.hint.RunHBTHint;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Objects;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class HbtTest implements MycatTest {
    private Connection connection;

    @Test
    public void testHBT() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(8066)) {
            execute(mycatConnection, RESET_CONFIG);
            execute(mycatConnection,
                    CreateDataSourceHint
                            .create("dw0",
                                    "jdbc:mysql://127.0.0.1:3306/mysql"));

            execute(mycatConnection,
                    CreateDataSourceHint
                            .create("dr0",
                                    "jdbc:mysql://127.0.0.1:3306/mysql"));

            execute(mycatConnection,
                    CreateDataSourceHint
                            .create("dw1",
                                    "jdbc:mysql://127.0.0.1:3307/mysql"));

            execute(mycatConnection,
                    CreateDataSourceHint
                            .create("dr1",
                                    "jdbc:mysql://127.0.0.1:3307"));

            execute(mycatConnection,
                    CreateClusterHint
                            .create("prototype",
                                    Arrays.asList("dw0"), Arrays.asList("dr0")));

            execute(mycatConnection,
                    CreateClusterHint
                            .create("prototype",
                                    Arrays.asList("dw1"), Arrays.asList("dr1")));

            execute(mycatConnection, "CREATE DATABASE db1");
            execute(mycatConnection, "USE `db1`;");
            execute(mycatConnection, "CREATE TABLE db1.`travelrecord` (\n" +
                    "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                    "  `user_id` varchar(100) DEFAULT NULL,\n" +
                    "  `traveldate` date DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int DEFAULT NULL,\n" +
                    "  `blob` longblob,\n" +
                    "  PRIMARY KEY (`id`),\n" +
                    "  KEY `id` (`id`)\n" +
                    ") ENGINE=InnoDB  DEFAULT CHARSET=utf8");

            deleteData(mycatConnection, "db1", "travelrecord");


            execute(mycatConnection, "CREATE TABLE `company` (\n" +
                    "  `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
                    "  `companyname` varchar(20) DEFAULT NULL,\n" +
                    "  `addressid` int(11) DEFAULT NULL,\n" +
                    "  PRIMARY KEY (`id`)\n" +
                    ") ENGINE=InnoDB AUTO_INCREMENT=235 DEFAULT CHARSET=utf8mb4;\n");
            deleteData(mycatConnection, "db1", "company");

            execute(mycatConnection, "INSERT INTO `db1`.`company` (`id`, `companyname`, `addressid`) VALUES ('1','Intel','1'),('2','IBM','2'),('3','Dell','3')");
            long max = 999999999;
            long min = 1;
            execute(mycatConnection, "INSERT INTO `db1`.`travelrecord` (id,`user_id`) VALUES (" + max + ",999)");
            execute(mycatConnection, "INSERT INTO `db1`.`travelrecord` (id,`user_id`) VALUES (" + min + ",999)");

            this.connection = mycatConnection;
            Assert.assertEquals("[{id0=1, id=1, companyname=Intel, addressid=1}]",
                    runHBT("innerJoin(`id0` = `id`,fromTable(db1,travelrecord).map(`id` as `id0`),fromTable(db1,company))"));

            //fromTable
            Assert.assertEquals(
                    "[{id=1, user_id=999, traveldate=null, fee=null, days=null, blob=null}, {id=999999999, user_id=999, traveldate=null, fee=null, days=null, blob=null}]",
                    runHBT("fromTable(db1,travelrecord).orderBy(order(id,ASC))"));

            //table
            Assert.assertEquals("[{id=1}, {id=2}, {id=3}]", runHBT("table(fields(fieldType(id,integer)),values(1,2,3))"));

            //fromSql
            Assert.assertEquals("[{1=1}]",runHBT("fromSql('prototype','select 1')"));

            //map
            Assert.assertEquals("[{$f0=null, id=1}, {$f0=3, id=1}]",runHBT("table(fields(fieldType(id,integer),fieldType(id2,integer)),values(1,2)).map(id + id2,id)"));

            //rename
            Assert.assertEquals("[{2=1, 1=null}, {2=1, 1=2}]",runHBT("table(fields(fieldType(`1`,`integer`,true),fieldType(`2`,`integer`,true)),values(1,2)).rename(`2`,`1`)"));

            //filter
            Assert.assertEquals("[{id=1, user_id=999, traveldate=null, fee=null, days=null, blob=null}]",runHBT("fromTable(db1,travelrecord).filter(`id` = 1)"));


            //集合操作测试
            Assert.assertEquals("[{1=1}, {1=2}]",runHBT("unionAll(fromSql('prototype','select 1'),fromSql('prototype','select 2'))"));

            //distinct
            Assert.assertEquals("[{1=1}]",runHBT("unionAll(fromSql('prototype','select 1'),fromSql('prototype','select 1')).distinct()"));

            //groupBy
            Assert.assertEquals("[{id=1, $f1=1.0}, {id=999999999, $f1=9.99999999E8}]",runHBT("fromTable(db1,travelrecord).groupBy(keys(groupKey(`id`)),aggregating(avg(`id`))))"));
            Assert.assertEquals("[{id=999999999, user_id=999, $f2=9.99999999E8}, {id=1, user_id=999, $f2=1.0}]",runHBT("fromTable(db1,travelrecord).groupBy(keys(groupKey(`id`,`user_id`)),aggregating(avg(`id`))))"));


            Assert.assertEquals("[{$f0=1000000000}]",runHBT("fromTable(db1,travelrecord).groupBy(keys(groupKey()),aggregating(sum(`id`)))"));

            Assert.assertEquals("[{1=1}]",runHBT("unionDistinct(fromSql('prototype','select 1'),fromSql('prototype','select 1'))"));
            Assert.assertEquals("[]",runHBT("exceptDistinct(fromSql('prototype','select 1'),fromSql('prototype','select 1'))"));
             Assert.assertEquals("[]",runHBT("exceptAll(fromSql('prototype','select 2'),fromSql('prototype','select 2'))"));
             Assert.assertEquals("[]",runHBT("intersectAll(fromSql('prototype','select 1'),fromSql('prototype','select 2'))"));
             Assert.assertEquals("[{2=2}]",runHBT("intersectDistinct(fromSql('prototype','select 2'),fromSql('prototype','select 2'))"));

            //EXPLAIN SELECT MAX(id) FROM db1.travelrecord;
             Assert.assertEquals("[{$f0=999999999}]",runHBT(
                     "fromTable(db1,travelrecord).groupBy(keys(groupKey()),aggregating(max(`id`)))"));

            //EXPLAIN SELECT COUNT(*) FROM db1.travelrecord;
             Assert.assertEquals("[{$f0=2}]",runHBT(
                     "fromTable(db1,travelrecord).groupBy(keys(groupKey()),aggregating(count()))"));

            //EXPLAIN SELECT COUNT(1) FROM db1.travelrecord;
             Assert.assertEquals("[{$f0=2}]",runHBT(
                     "unionAll( fromSql(prototype,'SELECT COUNT(*)  FROM `db1`.`travelrecord`'), fromSql(prototype,'SELECT COUNT(*)  FROM (SELECT NULL  FROM `db1`.`travelrecord`  UNION ALL  SELECT NULL  FROM `db1`.`travelrecord`  UNION ALL  SELECT NULL  FROM `db1`.`travelrecord`) AS `t2`')).groupBy(keys(groupKey()),aggregating(count()))"
             ));


            //order by
             Assert.assertEquals("[{id=1, user_id=999, traveldate=null, fee=null, days=null, blob=null}, {id=999999999, user_id=999, traveldate=null, fee=null, days=null, blob=null}]",
                     runHBT("fromTable(db1,travelrecord).orderBy(order(id,ASC))"));
             Assert.assertEquals("[{id=999999999, user_id=999, traveldate=null, fee=null, days=null, blob=null}, {id=1, user_id=999, traveldate=null, fee=null, days=null, blob=null}]",
                     runHBT("fromTable(db1,travelrecord).orderBy(order(id,DESC))"));

            //limit
             Assert.assertEquals("[{id=1, user_id=999, traveldate=null, fee=null, days=null, blob=null}]",
                     runHBT("fromTable(db1,travelrecord).orderBy(order(id,ASC)).limit(0,1)"));
             Assert.assertEquals("[{id=999999999, user_id=999, traveldate=null, fee=null, days=null, blob=null}]",
                     runHBT("fromTable(db1,travelrecord).orderBy(order(id,ASC)).limit(1,1)"));


             Assert.assertEquals("[{id0=1, id=1, companyname=Intel, addressid=1}, {id0=999999999, id=null, companyname=null, addressid=null}]",
                     runHBT("leftJoin(`id0` = `id`,fromTable(db1,travelrecord).map(`id` as `id0`),fromTable(db1,company)).orderBy(order(id,ASC))"));
             Assert.assertEquals("[{id0=1, id=1, companyname=Intel, addressid=1}, {id0=null, id=2, companyname=IBM, addressid=2}, {id0=null, id=3, companyname=Dell, addressid=3}]",
                     runHBT("rightJoin(`id0` = `id`,fromTable(db1,travelrecord).map(`id` as `id0`),fromTable(db1,company)).orderBy(order(id,ASC))"));
             Assert.assertEquals("[{id0=1}]",
                     runHBT("semiJoin(`id0` = `id`,fromTable(db1,travelrecord).map(`id` as `id0`),fromTable(db1,company)).orderBy(order(id0,ASC))"));
             Assert.assertEquals("[{id0=999999999}]",
                     runHBT("antiJoin(`id0` = `id`,fromTable(db1,travelrecord).map(`id` as `id0`),fromTable(db1,company)).orderBy(order(id0,ASC))"));
             Assert.assertEquals("[{id0=999999999}]",runHBT("antiJoin(`id0` = `id`,fromTable(db1,travelrecord).map(`id` as `id0`),fromTable(db1,company)).orderBy(order(id0,ASC))"));

            //三表
            Assert.assertEquals("[{id=1, user_id=999, traveldate=null, fee=null, days=null, blob=null, id0=1, companyname=Intel, addressid=1, id1=1, companyname0=Intel, addressid0=1}, {id=1, user_id=999, traveldate=null, fee=null, days=null, blob=null, id0=1, companyname=Intel, addressid=1, id1=1, companyname0=Intel, addressid0=1}, {id=1, user_id=999, traveldate=null, fee=null, days=null, blob=null, id0=1, companyname=Intel, addressid=1, id1=1, companyname0=Intel, addressid0=1}, {id=1, user_id=999, traveldate=null, fee=null, days=null, blob=null, id0=1, companyname=Intel, addressid=1, id1=1, companyname0=Intel, addressid0=1}, {id=999999999, user_id=999, traveldate=null, fee=null, days=null, blob=null, id0=null, companyname=null, addressid=null, id1=null, companyname0=null, addressid0=null}, {id=999999999, user_id=999, traveldate=null, fee=null, days=null, blob=null, id0=null, companyname=null, addressid=null, id1=null, companyname0=null, addressid0=null}, {id=999999999, user_id=999, traveldate=null, fee=null, days=null, blob=null, id0=null, companyname=null, addressid=null, id1=null, companyname0=null, addressid0=null}, {id=999999999, user_id=999, traveldate=null, fee=null, days=null, blob=null, id0=null, companyname=null, addressid=null, id1=null, companyname0=null, addressid0=null}]",
                    runHBT("leftJoin(`$0` eq `$$3`," +
                            "leftJoin(`$0` eq `$$3`," +
                            "unionAll( fromSql(prototype,'SELECT *  FROM `db1`.`travelrecord`'), " +
                            "fromSql(prototype,'SELECT *  FROM `db1`.`travelrecord`  UNION ALL  SELECT *  FROM `db1`.`travelrecord`  UNION ALL  SELECT *  FROM `db1`.`travelrecord`'))," +
                            "fromSql(prototype,'SELECT `id`, `companyname`, `addressid`, CAST(`id` AS SIGNED) AS `id0`  FROM `db1`.`company`'))" +
                            ".map(`$0` as `id`,`$1` as `user_id`,`$2` as `traveldate`,`$3` as `fee`,`$4` as `days`,`$5` as `blob`,`$6` as `id0`,`$7` as `companyname`,`$8` as `addressid`)" +
                            ",fromSql(prototype,'SELECT `id`, `companyname`, `addressid`, CAST(`id` AS SIGNED) AS `id0`  FROM `db1`.`company`'))" +
                            ".map(`$0` as `id`,`$1` as `user_id`,`$2` as `traveldate`,`$3` as `fee`,`$4` as `days`,`$5` as `blob`,`$6` as `id0`,`$7` as `companyname`,`$8` as `addressid`,`$9` as `id1`,`$10` as `companyname0`,`$11` as `addressid0`).orderBy(order(id,ASC))"));

            Assert.assertEquals(

                    "[{id=1, user_id=999, traveldate=null, fee=null, days=null, blob=null, id0=1, user_id0=999, traveldate0=null, fee0=null, days0=null, blob0=null, id1=1, companyname=Intel, addressid=1}, {id=999999999, user_id=999, traveldate=null, fee=null, days=null, blob=null, id0=999999999, user_id0=999, traveldate0=null, fee0=null, days0=null, blob0=null, id1=null, companyname=null, addressid=null}]",
                    runHBT("leftJoin(`$0` eq `$$0`," +
                            "leftJoin(`$0` eq `$$0`,fromTable(db1,travelrecord), fromTable(db1,travelrecord))" +
                            ",fromTable(db1,company)).orderBy(order(id,ASC))"));
             Assert.assertEquals("[{id=1, user_id=999, traveldate=null, fee=null, days=null, blob=null}]",runHBT("filterFromTable(`id` = 1,db1,travelrecord).orderBy(order(id,ASC))"));
             Assert.assertEquals("[{id=1}]",runHBT("fromRelToSql(prototype,fromTable('db1','travelrecord').filter(`id` = 1).map(`id`)).orderBy(order(id,ASC))"));

            System.out.println();
        }
    }

    private String runHBT(String hbt) throws Exception {
        return Objects.toString(
                executeQuery(this.connection,
                        RunHBTHint.create(hbt)));
    }
}
