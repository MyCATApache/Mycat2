package io.mycat.springdata;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.expr.SQLExprUtils;
import com.alibaba.fastsql.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import io.mycat.assemble.MycatTest;
import io.mycat.hint.CreateClusterHint;
import io.mycat.hint.CreateDataSourceHint;
import io.mycat.hint.SetUserDialectHint;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.jpa.domain.JpaSort;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public abstract class TableJPATemplateTest implements MycatTest {

     Class clazz;
     ConfigurableApplicationContext applicationContext;
     CustomerRepository repository;
     String sql;
     CreateTableSQLType createTableSQLType;
     String dialect;
    private String dbtype;

    public static enum CreateTableSQLType {
        GLOBAL,
        SHARDING,
        NORMAL
    }

    public TableJPATemplateTest(String dbtype,Class clazz) {
        this.dbtype = dbtype;
        this.clazz = clazz;
    }

    public void initDb() throws Exception {
        if ("mysql".equalsIgnoreCase(dbtype)) {
            dialect = "org.hibernate.dialect.MySQL8Dialect";
        } else if ("oracle".equalsIgnoreCase(dbtype)) {
            dialect = "org.hibernate.dialect.OracleDialect";
        } else if ("sqlserver".equalsIgnoreCase(dbtype)) {
            dialect = "org.hibernate.dialect.SQLServerDialect";
        } else if ("PostgreSQL".equalsIgnoreCase(dbtype)) {
            dialect = "org.hibernate.dialect.PostgreSQL82Dialect";
        }
        try (Connection mySQLConnection = getMySQLConnection(8066)) {
            execute(mySQLConnection, RESET_CONFIG);
            execute(mySQLConnection, "drop database IF EXISTS db1");
            execute(mySQLConnection, "create database IF NOT EXISTS db1");
        }
    }
    public void runTable(Class clazz, CreateTableSQLType createTableSQLType) throws Exception {
        sql = "CREATE TABLE db1.`customer` (\n" +
                "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                "  `firstname` varchar(100) DEFAULT NULL,\n" +
                "  `lastname` varchar(100) DEFAULT NULL,\n" +
                "  `modified_date` date  DEFAULT NULL,\n" +
                "  `created_date` date DEFAULT NULL\n , " +
                "   PRIMARY KEY (`id`)" +
                ") ENGINE=InnoDB  DEFAULT CHARSET=utf8";
        this.createTableSQLType = createTableSQLType;
        MySqlCreateTableStatement sqlStatement = (MySqlCreateTableStatement) SQLUtils.parseSingleMysqlStatement(sql);
        switch (createTableSQLType) {
            case GLOBAL:
                sqlStatement.setBroadCast(true);
                this.sql = sqlStatement.toString();
                break;
            case SHARDING:
                sqlStatement.setTablePartitions(SQLExprUtils.fromJavaObject(2));
                sqlStatement.setTablePartitionBy(
                        new SQLMethodInvokeExpr("hash",
                                null, new SQLIdentifierExpr("id"))
                );
                sqlStatement.setDbPartitions(SQLExprUtils.fromJavaObject(2));
                sqlStatement.setDbPartitionBy(
                        new SQLMethodInvokeExpr("hash",
                                null, new SQLIdentifierExpr("id"))
                );
                this.sql = sqlStatement.toString();
                break;
            case NORMAL:
                this.sql = sql;
                break;
        }

        try (Connection mySQLConnection = getMySQLConnection(8066)) {

            switch (this.createTableSQLType) {

                case GLOBAL:
                    execute(mySQLConnection, CreateDataSourceHint
                            .create("newDs",
                                    "jdbc:mysql://127.0.0.1:3306/mysql"));
                    execute(mySQLConnection, CreateClusterHint.create("c0", Arrays.asList("newDs"), Collections.emptyList()));
                    break;

                case SHARDING:
                    execute(mySQLConnection, CreateDataSourceHint
                            .create("newDs",
                                    "jdbc:mysql://127.0.0.1:3306/mysql"));
                    execute(mySQLConnection, CreateClusterHint.create("c0", Arrays.asList("newDs"), Collections.emptyList()));

                    execute(mySQLConnection, CreateDataSourceHint
                            .create("newDs2",
                                    "jdbc:mysql://127.0.0.1:3307"));
                    execute(mySQLConnection, CreateClusterHint.create("c1", Arrays.asList("newDs2"), Collections.emptyList()));
                    break;

                case NORMAL:
                    break;
            }
            execute(mySQLConnection, this.sql);
        }
    }


    @Test
    public void testSaveOne() {
        repository.deleteAllInBatch();
        Customer customer = new Customer();
        repository.save(customer);
        Optional<Customer> lastname = repository.findByLastname(customer.lastname);
        Assert.assertTrue(lastname.isPresent());
    }

    @Test
    public void testSaveAll() {
        repository.deleteAll();
        List<Customer> customerList = IntStream.range(0, 10)
                .mapToObj(i -> String.valueOf(i))
                .map(i -> {
                    Customer customer = new Customer();
                    customer.lastname = i;
                    return customer;
                }).collect(Collectors.toList());
        repository.saveAll(customerList);
        customerList.sort(Comparator.comparing(x->x.id));
        List<Customer> all = repository.findAll();
        all.sort(Comparator.comparing(x->x.id));
        Assert.assertEquals(customerList, all);
    }

    @Test
    public void testDeleteAllInBatch() {
        repository.deleteAllInBatch();
        List<Customer> customerList = IntStream.range(0, 10)
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
        List<Customer> customerList = IntStream.range(0, 10)
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
        customerList = customerList.stream().sorted(Comparator.comparing(i -> i.lastname)).collect(Collectors.toList());
        List<Customer> customers = repository.findAll(JpaSort.by("lastname"));
        Assert.assertEquals(customerList, customers);
        repository.deleteAllInBatch();
    }

    @Test
    public void testCount() {
        repository.deleteAllInBatch();
        List<Customer> customerList = IntStream.range(1, 10)
                .mapToObj(i -> {
                    Customer customer = new Customer();
                    customer.id = Long.valueOf(i);
                    customer.lastname = i + "";
                    return customer;
                }).collect(Collectors.toList());
        repository.saveAll(customerList);
        Assert.assertEquals(customerList.size(), repository.count());
        repository.deleteAllInBatch();
    }

}