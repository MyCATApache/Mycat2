package io.mycat.springdata;

import io.mycat.assemble.MycatTest;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;
import org.springframework.scheduling.annotation.EnableAsync;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;

import static com.alibaba.druid.util.JdbcUtils.execute;

@EnableAsync
@SpringBootApplication
@EnableJpaAuditing
class AuditingConfiguration {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");  //注册数据库驱动
        String url = "jdbc:mysql://localhost:8066?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true";
        //定义连接数据库的url
        try(Connection conn = DriverManager.getConnection(url,"root","123456");){
            execute(conn, "create database IF NOT EXISTS db1");
        }

        SpringApplication springApplication = new SpringApplication(AuditingConfiguration.class);
        ConfigurableApplicationContext applicationContext = springApplication.run();
        CustomerRepository bean = applicationContext.getBean(CustomerRepository.class);
        Customer customer = new Customer();
        customer.lastname = "1";
        bean.save(customer);
        Optional<Customer> byLastname = bean.findByLastname("1");
        System.out.println();
    }
}