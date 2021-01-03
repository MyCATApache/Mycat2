package io.mycat.springdata;

import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
@SpringBootApplication
public class MySQLNormalTableJPATest extends NormalTableJPATest {

    public MySQLNormalTableJPATest() {
        super("mysql", MySQLNormalTableJPATest.class);
    }
}
