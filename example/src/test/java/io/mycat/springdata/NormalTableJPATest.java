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
        initDb();
        this.applicationContext = new SpringApplication(GlobalTableJPATest.class).run();
        this.repository = applicationContext.getBean(CustomerRepository.class);
        runTable(NormalTableJPATest.class,CreateTableSQLType.NORMAL);

    }
}