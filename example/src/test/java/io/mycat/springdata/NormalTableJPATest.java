package io.mycat.springdata;

import org.junit.Before;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;


public abstract class NormalTableJPATest extends TableJPATemplateTest  {
    public NormalTableJPATest(String dbtype, Class clazz) {
        super(dbtype, clazz);
    }

    @Before
    public void before() throws Exception {
        initDb();
        this.applicationContext = new SpringApplication(clazz).run();
        this.repository = applicationContext.getBean(CustomerRepository.class);
        runTable(NormalTableJPATest.class,CreateTableSQLType.NORMAL);

    }
}