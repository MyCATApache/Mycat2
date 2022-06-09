package io.mycat.hint;

import com.alibaba.druid.sql.parser.SQLType;
import io.mycat.assemble.MycatTest;
import io.mycat.config.UserConfig;
import org.jetbrains.annotations.NotNull;
import org.junit.Ignore;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
@Ignore
public class UserHintTest implements MycatTest {

    @Test()
    public void emptyTest() throws Exception {
        UserConfig.RoleConfig roleConfig = new UserConfig.RoleConfig();
        UserConfig userConfig = new UserConfig();
        userConfig.setUsername("root");
        userConfig.setRole(roleConfig);
        String text = CreateUserHint.create(userConfig);

        Connection mycatConnection = DriverManager.getConnection(DB_MYCAT, "root", "123456");
        try {
            execute(mycatConnection, text);
            testSql(mycatConnection);
        } finally {
            execute(mycatConnection, RESET_CONFIG);
            mycatConnection.close();
        }

    }

    @Test(expected = Exception.class)
    public void forbiddenDrop() throws Exception {

        Connection mycatConnection = DriverManager.getConnection(DB_MYCAT, "root", "123456");
        try {
            UserConfig userConfig = getUserConfigWithRootPrivilege();
            String text = CreateUserHint.create(userConfig);

            execute(mycatConnection, text);
            execute(mycatConnection, "DROP DATABASE db1");
            execute(mycatConnection, "CREATE DATABASE db1");
            System.out.println(text);
        } finally {
            execute(mycatConnection, RESET_CONFIG);
            mycatConnection.close();
        }
    }

    @Test()
    public void allowCreate() throws Exception {
        Connection mycatConnection = DriverManager.getConnection(DB_MYCAT, "root", "123456");
        try {
            UserConfig userConfig = getUserConfigWithRootPrivilege();
            String text = CreateUserHint.create(userConfig);

            execute(mycatConnection, text);
            execute(mycatConnection, "CREATE DATABASE db1");
            System.out.println(text);
        } finally {
            execute(mycatConnection, RESET_CONFIG);
            mycatConnection.close();
        }
    }

    @NotNull
    private UserConfig getUserConfigWithRootPrivilege() {
        UserConfig.RoleConfig roleConfig = new UserConfig.RoleConfig();
        roleConfig.setDisallowSqlTypes(Stream.of(SQLType.DROP.name(), SQLType.CREATE.name()).collect(Collectors.toSet()));
        UserConfig userConfig = new UserConfig();
        userConfig.setUsername("root");
        userConfig.setPassword("123456");
        userConfig.setRole(roleConfig);
        return userConfig;
    }
//
//    @NotNull
//    private UserConfig getUserConfigWithSchemaDb1TravelrecordPrivilege() {
//        UserConfig userConfig = getUserConfigWithSchemaDb1Privilege();
//        List<UserConfig.TablePrivilege> tablePrivileges = userConfig.getRole().getSchemaPrivileges().get(0).getTablePrivileges();
//        UserConfig.TablePrivilege tablePrivilege = new UserConfig.TablePrivilege();
//        tablePrivilege.setName("travelrecord");
//        tablePrivileges.add(tablePrivilege);
//
//        List<String> allowSqlTypes = tablePrivilege.getAllowSqlTypes();
//        List<String> disallowSqlTypes = tablePrivilege.getDisallowSqlTypes();
//        disallowSqlTypes.add(SQLType.INSERT.name());
//        allowSqlTypes.add(SQLType.SELECT.name());
//        return userConfig;
//    }
//
//    @NotNull
//    private UserConfig getUserConfigWithSchemaDb1Privilege() {
//        UserConfig userConfig = getUserConfigWithRoleDb1();
//        List<UserConfig.SchemaPrivilege> schemaPrivileges = userConfig.getRole().getSchemaPrivileges();
//        UserConfig.SchemaPrivilege schemaPrivilege = schemaPrivileges.get(0);
//        List<String> allowSqlTypes = schemaPrivilege.getAllowSqlTypes();
//        List<String> disallowSqlTypes = schemaPrivilege.getDisallowSqlTypes();
//        disallowSqlTypes.add(SQLType.UNKNOWN.name());
//        disallowSqlTypes.add(SQLType.DROP.name());
//        allowSqlTypes.add(SQLType.CREATE.name());
//        return userConfig;
//    }

//    @NotNull
//    private UserConfig getUserConfigWithRoleDb1() {
//        List<UserConfig.SchemaPrivilege> schemaPrivileges = new ArrayList<>();
//        UserConfig.SchemaPrivilege schemaPrivilege = new UserConfig.SchemaPrivilege();
//        schemaPrivilege.setName("db1");
//        schemaPrivileges.add(schemaPrivilege);
//        UserConfig.RoleConfig roleConfig = new UserConfig.RoleConfig();
//        roleConfig.setSchemaPrivileges(schemaPrivileges);
//        UserConfig userConfig = new UserConfig();
//        userConfig.setUsername("root");
//        userConfig.setPassword("123456");
//        userConfig.setRole(roleConfig);
//        return userConfig;
//    }


    private void testSql(Connection mycatConnection) throws Exception {
        execute(mycatConnection, "DROP DATABASE db1");


        execute(mycatConnection, "CREATE DATABASE db1");

        execute(mycatConnection, CreateDataSourceHint
                .create("ds0",
                        DB1));
        execute(mycatConnection, CreateDataSourceHint
                .create("ds1",
                        DB2));

        execute(mycatConnection,
                CreateClusterHint.create("c0",
                        Arrays.asList("ds0"), Collections.emptyList()));
        execute(mycatConnection,
                CreateClusterHint.create("c1",
                        Arrays.asList("ds1"), Collections.emptyList()));

        execute(mycatConnection, "USE `db1`;");

        execute(mycatConnection, "CREATE TABLE db1.`travelrecord` (\n" +
                "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                "  `user_id` varchar(100) DEFAULT NULL,\n" +
                "  `traveldate` date DEFAULT NULL,\n" +
                "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                "  `days` int DEFAULT NULL,\n" +
                "  `blob` longblob,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  KEY `id` (`id`)\n" +
                ") ENGINE=InnoDB  DEFAULT CHARSET=utf8");
        //execute(mycatConnection, "CREATE TABLE `company` ( `id` int(11) NOT NULL AUTO_INCREMENT,`companyname` varchar(20) DEFAULT NULL,`addressid` int(11) DEFAULT NULL,PRIMARY KEY (`id`))");
        execute(mycatConnection, "delete from db1.travelrecord");

        for (int i = 1; i < 10; i++) {
            execute(mycatConnection, "insert db1.travelrecord (id) values(" + i + ")");
        }
    }

}
