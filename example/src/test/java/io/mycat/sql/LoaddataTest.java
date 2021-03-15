package io.mycat.sql;

import io.mycat.assemble.MycatTest;
import io.mycat.hint.CreateClusterHint;
import io.mycat.hint.CreateDataSourceHint;
import io.mycat.hint.LoaddataHint;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class LoaddataTest implements MycatTest {

    @Test
    public void testLoaddata() throws Exception {
        try(Connection mycatConnection = getMySQLConnection(DB_MYCAT)){
            execute(mycatConnection, RESET_CONFIG);

            execute(mycatConnection, "DROP DATABASE db1");


            execute(mycatConnection, "CREATE DATABASE db1");


            execute(mycatConnection, CreateDataSourceHint
                    .create("ds0",
                            DB1));

            execute(mycatConnection,
                    CreateClusterHint.create("c0",
                            Arrays.asList("ds0"), Collections.emptyList()));

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
                    ") ENGINE=InnoDB  DEFAULT CHARSET=utf8"
                    + " dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2 dbpartitions 2;");
            deleteData(mycatConnection,"db1","travelrecord");

            CSVFormat format = CSVFormat.MYSQL;
            Path mycat_loaddata_example = Files.createTempFile("", "mycat_loaddata_example");
            StringBuilder sb = new StringBuilder();
            CSVPrinter printer = format.print(sb);
            printer.printRecord(new Object[]{"1", null, null, null, null, null});
            printer.printRecord(new Object[]{"2", null, null, null, null, null});
            printer.printRecord(new Object[]{"3", null, null, null, null, null});
            Files.write(mycat_loaddata_example, sb.toString().getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);


            execute(mycatConnection, LoaddataHint.create("db1", "travelrecord", mycat_loaddata_example.toString()));

            List<Map<String, Object>> maps = executeQuery(mycatConnection, "select * from db1.travelrecord");
            Assert.assertEquals(3, maps.size());
        }
    }

    @Test
    public void testLoaddata2() throws Exception {
        try(Connection mycatConnection = getMySQLConnection(DB_MYCAT)){
            execute(mycatConnection, RESET_CONFIG);

            execute(mycatConnection, "DROP DATABASE db1");


            execute(mycatConnection, "CREATE DATABASE db1");


            execute(mycatConnection, CreateDataSourceHint
                    .create("ds0",
                            DB1));

            execute(mycatConnection,
                    CreateClusterHint.create("c0",
                            Arrays.asList("ds0"), Collections.emptyList()));

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
                    ") ENGINE=InnoDB  DEFAULT CHARSET=utf8"
                    + " dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2 dbpartitions 2;");
            deleteData(mycatConnection,"db1","travelrecord");

            CSVFormat format = CSVFormat.MYSQL.withDelimiter(',');
            Path mycat_loaddata_example = Files.createTempFile("", "mycat_loaddata_example");
            StringBuilder sb = new StringBuilder();
            CSVPrinter printer = format.print(sb);
            printer.printRecord(new Object[]{"1", null, null, null, null, null});
            printer.printRecord(new Object[]{"2", null, null, null, null, null});
            printer.printRecord(new Object[]{"3", null, null, null, null, null});
            Files.write(mycat_loaddata_example, sb.toString().getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);


            execute(mycatConnection, LoaddataHint.create("db1", "travelrecord",
                    mycat_loaddata_example.toString(),Collections.singletonMap("delimiter",",")));

            List<Map<String, Object>> maps = executeQuery(mycatConnection, "select * from db1.travelrecord");
            Assert.assertEquals(3, maps.size());
        }


    }
}
