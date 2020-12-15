package io.mycat.springdata;

import org.junit.Before;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
@SpringBootApplication
public class GlobalTableJPATest extends TableJPATemplateTest {

    @Before
    public void before() throws Exception {
        try (Connection mySQLConnection = getMySQLConnection(8066)) {
            execute(mySQLConnection, "drop database IF EXISTS db1");
            execute(mySQLConnection, "create database IF NOT EXISTS db1");
        }

        SpringApplication springApplication = new SpringApplication(GlobalTableJPATest.class);
        this.applicationContext = springApplication.run();
        runInitSQL(GlobalTableJPATest.class,CreateTableSQLType.GLOBAL);
        this.repository = applicationContext.getBean(CustomerRepository.class);
    }

}