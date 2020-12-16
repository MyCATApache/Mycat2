package io.mycat.springdata;

import org.junit.Before;
import org.springframework.boot.SpringApplication;

import java.util.Collections;


public abstract class GlobalTableJPATest extends TableJPATemplateTest {


    public GlobalTableJPATest(String dbtype,Class clazz) {
        super(dbtype,clazz);
    }

    @Before
    public void before() throws Exception {
        initDb();
        SpringApplication springApplication = new SpringApplication(clazz);
        springApplication.setDefaultProperties(Collections.singletonMap(
                "spring.jpa.properties.hibernate.dialect", dialect));
        this.applicationContext = springApplication.run();

        this.repository = applicationContext.getBean(CustomerRepository.class);
        runTable(clazz, CreateTableSQLType.GLOBAL);
    }


}