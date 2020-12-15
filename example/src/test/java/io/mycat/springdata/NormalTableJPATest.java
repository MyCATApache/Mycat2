package io.mycat.springdata;

import io.mycat.assemble.MycatTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.jpa.domain.JpaSort;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;
import org.springframework.scheduling.annotation.EnableAsync;

import java.sql.Connection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static com.alibaba.druid.util.JdbcUtils.execute;

@SpringBootApplication
public class NormalTableJPATest implements MycatTest {

    private ConfigurableApplicationContext applicationContext;
    private CustomerRepository repository;

    @Before
    public  void before() throws Exception {
        try (Connection mySQLConnection = getMySQLConnection(8066)) {
            execute(mySQLConnection, "drop database IF EXISTS db1");
            execute(mySQLConnection, "create database IF NOT EXISTS db1");
        }
        SpringApplication springApplication = new SpringApplication(NormalTableJPATest.class);
        try (Connection mySQLConnection = getMySQLConnection(8066)) {
            execute(mySQLConnection, "CREATE TABLE db1.`customer` (\n" +
                    "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                    "  `firstname` varchar(100) DEFAULT NULL,\n" +
                    "  `lastname` varchar(100) DEFAULT NULL,\n" +
                    "  `createdDate` date  DEFAULT NULL,\n" +
                    "  `createdDate` date DEFAULT NULL\n" +
                    ") ENGINE=InnoDB  DEFAULT CHARSET=utf8"
                    + " dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2 dbpartitions 2;");

        }
        this.applicationContext = springApplication.run();
        this.repository = applicationContext.getBean(CustomerRepository.class);
    }

    @Test
    public void testSaveOne() {
        Customer customer = new Customer();
        customer.lastname = ThreadLocalRandom.current().nextInt() + "";
        repository.delete(customer);
        repository.save(customer);
        Optional<Customer> lastname = repository.findByLastname(customer.lastname);
        Assert.assertTrue(lastname.isPresent());
    }

    @Test
    public void testSaveAll() {
        repository.deleteAll();
        List<Customer> customerList = ThreadLocalRandom.current().ints(10)
                .mapToObj(i -> String.valueOf(i))
                .map(i -> {
                    Customer customer = new Customer();
                    customer.lastname = i;
                    return customer;
                }).collect(Collectors.toList());
        repository.saveAll(customerList);
        Assert.assertEquals(customerList,repository.findAll());
    }

    @Test
    public void testDeleteAllInBatch() {
        List<Customer> customerList = ThreadLocalRandom.current().ints(10)
                .mapToObj(i -> String.valueOf(i))
                .map(i -> {
                    Customer customer = new Customer();
                    customer.lastname = i;
                    return customer;
                }).collect(Collectors.toList());
        repository.saveAll(customerList);
        repository.deleteAllInBatch();
    }

    @Test
    public void testFindAllSort() {
        repository.deleteAllInBatch();
        List<Customer> customerList = ThreadLocalRandom.current().ints(10)
                .mapToObj(i -> String.valueOf(i))
                .map(i -> {
                    Customer customer = new Customer();
                    customer.lastname = i;
                    return customer;
                })
                .collect(Collectors.toList());
        Collections.shuffle(customerList);
        repository.saveAll(customerList);
        repository.flush();
        customerList=  customerList.stream().sorted(Comparator.comparing(i->i.lastname)).collect(Collectors.toList());
        List<Customer> customers = repository.findAll(JpaSort.by("lastname"));
        Assert.assertEquals(customerList,customers);
        repository.deleteAllInBatch();
    }
    @Test
    public void testCount() {
        repository.deleteAllInBatch();
        List<Customer> customerList = ThreadLocalRandom.current().ints(10)
                .mapToObj(i -> String.valueOf(i))
                .map(i -> {
                    Customer customer = new Customer();
                    customer.lastname = i;
                    return customer;
                }).collect(Collectors.toList());
        repository.saveAll(customerList);
        Assert.assertEquals(10,repository.count());
        repository.deleteAllInBatch();
    }

}