package io.mycat.springdata;

import org.junit.Before;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
@SpringBootApplication
public class NormalTableJPATest extends TableJPATemplateTest  {

    @Before
    public void before() throws Exception {
        SpringApplication springApplication = new SpringApplication(NormalTableJPATest.class);
        this.applicationContext = springApplication.run();
        runInitSQL(NormalTableJPATest.class,CreateTableSQLType.NORMAL);
        this.repository = applicationContext.getBean(CustomerRepository.class);
    }
}