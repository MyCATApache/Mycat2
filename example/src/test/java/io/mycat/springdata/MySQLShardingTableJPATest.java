package io.mycat.springdata;

import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
@SpringBootApplication
public class MySQLShardingTableJPATest extends ShardingTableJPATest {

    public MySQLShardingTableJPATest() {
        super("mysql", MySQLShardingTableJPATest.class);
    }
}
