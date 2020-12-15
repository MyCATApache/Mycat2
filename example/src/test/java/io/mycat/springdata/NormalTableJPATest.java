package io.mycat.springdata;

import org.junit.Before;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
@SpringBootApplication
public class NormalTableJPATest extends TableJPATemplateTest  {

    @Before
    public void before() throws Exception {
        try (Connection mySQLConnection = getMySQLConnection(8066)) {
            execute(mySQLConnection, "create database IF NOT EXISTS db1");
        }
        SpringApplication springApplication = new SpringApplication(NormalTableJPATest.class);
        this.applicationContext = springApplication.run();
        runInitSQL(NormalTableJPATest.class,CreateTableSQLType.NORMAL);
        this.repository = applicationContext.getBean(CustomerRepository.class);
    }
}