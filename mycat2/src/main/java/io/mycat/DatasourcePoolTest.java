package io.mycat;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.google.common.collect.ImmutableList;
import io.mycat.beans.mycat.MycatRelDataType;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.commands.JdbcDatasourcePoolImpl;
import io.mycat.commands.MycatDatasourcePool;
import io.mycat.commands.VertxMySQLDatasourcePoolImpl;
import io.mycat.config.DatasourceConfig;
import io.mycat.datasource.jdbc.DruidDatasourceProvider;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.newquery.MysqlCollector;
import io.mycat.newquery.NewMycatConnection;
import io.mycat.newquery.RowSet;
import io.mycat.util.JsonUtil;
import io.reactivex.rxjava3.core.Observable;
import lombok.SneakyThrows;
import lombok.ToString;
import org.apache.arrow.memory.*;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.groovy.util.Maps;
import org.jetbrains.annotations.NotNull;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static io.mycat.PreparedStatement.fromJavaObject;

public class DatasourcePoolTest {

    @SneakyThrows
    public static void main(String[] args) {

        DatasourceConfig datasourceConfig = new DatasourceConfig();
        datasourceConfig.setDbType("mysql");
        datasourceConfig.setUser("root");
        datasourceConfig.setPassword("123456");
        datasourceConfig.setName("prototypeDs");
        datasourceConfig.setUrl("jdbc:mysql://localhost:3307/mysql");
        datasourceConfig.setMaxCon(10);
        Map<String, DatasourceConfig> datasources = Maps.of("prototypeDs", datasourceConfig);
        MetaClusterCurrent.register(JdbcConnectionManager.class, new JdbcConnectionManager(
                datasources, new DruidDatasourceProvider()));
        MetaClusterCurrent.register(IOExecutor.class, IOExecutor.DEFAULT);

        MycatDatasourcePool jdbcDs =
                new JdbcDatasourcePoolImpl("prototypeDs");


        NewMycatConnection newMycatConnection = jdbcDs.getConnection().toCompletionStage().toCompletableFuture().get();
        Observable<VectorSchemaRoot> observable = newMycatConnection.prepareQuery("select 1", Collections.emptyList(), new RootAllocator());
        List<VectorSchemaRoot> vectorSchemaRoots = observable.toList().blockingGet();

        System.out.println();

        /////////////////////////////////////////////////////////////////////////////////////////////////////////////
        MycatDatasourcePool nativeDs =
               new VertxMySQLDatasourcePoolImpl(datasourceConfig,"prototypeDs");


        MycatRelDataType sqlMycatRelType = getSqlMycatRelDataType();

        TestResult testResult = getTestResult(jdbcDs);
        TestResult testResult2 = getTestResult(nativeDs);
        //List<String> collect = newMycatConnection.prepareQuery(querySql, Collections.emptyList()).toList().blockingGet().stream().map(i -> i.toString()).collect(Collectors.toList());
        MycatRowMetaData mycatRowMetaData1 = testResult.queryResult.getMycatRowMetaData();
        MycatRowMetaData mycatRowMetaData2 = testResult2.queryResult.getMycatRowMetaData();
        MycatRelDataType mycatRelDataType1 =mycatRowMetaData1.getMycatRelDataType();
        MycatRelDataType mycatRelDataType2 = mycatRowMetaData2 .getMycatRelDataType();

        System.out.println(mycatRelDataType1);
        System.out.println(mycatRelDataType2);
        System.out.println("=====================================================================================================");
        boolean equals = mycatRelDataType1.equals(mycatRelDataType2);
//        Assert.assertTrue(equals);

        equals = mycatRelDataType1.equals(sqlMycatRelType);

        System.out.println(sqlMycatRelType);
        System.out.println(mycatRelDataType1);

     observable = newMycatConnection.prepareQuery("select * from testSchema.testColumnTable", Collections.emptyList(), new RootAllocator());
   vectorSchemaRoots = observable.toList().blockingGet();

        System.out.println();
    }

    @NotNull
    private static MycatRelDataType getSqlMycatRelDataType() {
        String sql = getSql("s", "t");
        MySqlCreateTableStatement sqlStatement = (MySqlCreateTableStatement)SQLUtils.parseSingleMysqlStatement(sql);
        MycatRelDataType mycatRelType = MycatRelDataType.getMycatRelType(sqlStatement);
        return mycatRelType;
    }

    @NotNull
    private static TestResult getTestResult(MycatDatasourcePool prototypeDs) throws InterruptedException, ExecutionException {
        String schema = "testSchema";
        String table = "testColumnTable";
        String sql = getSql(schema, table);


        System.out.println(sql);
        NewMycatConnection newMycatConnection = prototypeDs.getConnection().toCompletionStage().toCompletableFuture().get();
        newMycatConnection.update(sql).toCompletionStage().toCompletableFuture().get();

        MySqlInsertStatement mySqlInsertStatement = new MySqlInsertStatement();
        SQLExprTableSource sqlExprTableSource = new SQLExprTableSource();
        sqlExprTableSource.setSimpleName(table);
        sqlExprTableSource.setSchema(schema);
        mySqlInsertStatement.setTableSource(sqlExprTableSource);

        ImmutableList<Object[]> columns = getColumns();
        for (Object[] objects :  columns) {
            String column = (String) objects[0];
            String type = (String) objects[1];
            mySqlInsertStatement.addColumn(new SQLIdentifierExpr("`" + column + "`"));
        }
        SQLInsertStatement.ValuesClause valuesClause = new SQLInsertStatement.ValuesClause();
        for (Object[] objects :  columns) {
            String column = (String) objects[0];
            String type = (String) objects[1];
            Object value = objects[2];
            valuesClause.addValue(fromJavaObject(value));
        }
        mySqlInsertStatement.setValues(valuesClause);
        newMycatConnection.update("delete from "+ schema + "." + table).toCompletionStage().toCompletableFuture().get();

        newMycatConnection.update(mySqlInsertStatement.toString()).toCompletionStage().toCompletableFuture().get();
        String querySql = "select * from " + schema + "." + table;
        RowSet queryResult = newMycatConnection.query(querySql).toCompletionStage().toCompletableFuture().get();
        StringMysqlCollector collector = new StringMysqlCollector() ;
        newMycatConnection.prepareQuery(querySql, Collections.emptyList(), collector);
        collector.await();
        TestResult testResult = TestResult.of(queryResult, collector);
        return testResult;
    }

    private static String getSql(String schema, String table) {
        MySqlCreateTableStatement createTableStatement = new MySqlCreateTableStatement();
        createTableStatement.setTableName(table);
        createTableStatement.setSchema(schema);
        createTableStatement.setIfNotExiists(true);

        for (Object[] objects :  getColumns()) {
            String column = (String) objects[0];
            String type = (String) objects[1];
            createTableStatement.addColumn("`" + column + "`", type);
        }
        String sql = createTableStatement.toString();
        return sql;
    }

    public static ImmutableList<Object[]> getColumns() {
        ImmutableList.Builder<Object[]> builder = ImmutableList.builder();
        builder.add(new Object[]{"bit1", "BIT(1)", 1});
        builder.add(new Object[]{"bit2", "BIT(2)", 1});
        builder.add(new Object[]{"BOOL", "BOOL", true});

        builder.add(new Object[]{"TINYINT1", "TINYINT(1)", 1});
        builder.add(new Object[]{"TINYINT2", "TINYINT(2)", 2});
        builder.add(new Object[]{"UTINYINT2", "TINYINT(2) UNSIGNED", 2});


        builder.add(new Object[]{"SMALLINT", "SMALLINT", -1});
        builder.add(new Object[]{"MEDIUMINT", "MEDIUMINT", 1});
        builder.add(new Object[]{"INTEGER", "INTEGER", 1});
        builder.add(new Object[]{"BIGINT", "BIGINT", 1});

        builder.add(new Object[]{"USMALLINT", "SMALLINT UNSIGNED", 1});
        builder.add(new Object[]{"UMEDIUMINT", "MEDIUMINT UNSIGNED", 1});
        builder.add(new Object[]{"UINTEGER", "INTEGER UNSIGNED", 1});
        builder.add(new Object[]{"UBIGINT", "BIGINT UNSIGNED", 1});
        builder.add(new Object[]{"FLOAT", "FLOAT", 1});
        builder.add(new Object[]{"DOUBLE", "DOUBLE", 1});
        builder.add(new Object[]{"DECIMAL", "DECIMAL", 1});
        builder.add(new Object[]{"DATE", "DATE", Date.valueOf(LocalDate.now())});
        builder.add(new Object[]{"DATETIME", "DATETIME", Timestamp.valueOf(LocalDateTime.now())});
        builder.add(new Object[]{"TIMESTAMP", "TIMESTAMP", Timestamp.valueOf(LocalDateTime.now())});
        builder.add(new Object[]{"TIME", "TIME", Time.valueOf(LocalTime.now())});
        builder.add(new Object[]{"YEAR", "YEAR", 1992});
        builder.add(new Object[]{"CHAR2", "CHAR(2)", "aa"});
        builder.add(new Object[]{"VARCHAR", "VARCHAR(2)", "aa"});
        builder.add(new Object[]{"BINARY", "BINARY(2)", new byte[]{'a', 'a'}});
        builder.add(new Object[]{"BCHAR", "CHAR(2) BINARY", new byte[]{'a', 'a'}});
        builder.add(new Object[]{"VARBINARY", "VARBINARY(2)", new byte[]{'a', 'a'}});
        builder.add(new Object[]{"BVARCHAR", "VARCHAR(2) BINARY", new byte[]{'a', 'a'}});
        builder.add(new Object[]{"BLOB", "BLOB", new byte[]{'a', 'a'}});
        builder.add(new Object[]{"TINYBLOB", "TINYBLOB", new byte[]{'a', 'a'}});
        builder.add(new Object[]{"MEDIUMBLOB", "MEDIUMBLOB", new byte[]{'a', 'a'}});
        builder.add(new Object[]{"LONGBLOB", "LONGBLOB", new byte[]{'a', 'a'}});
        builder.add(new Object[]{"TEXT", "TEXT", "aa"});
        builder.add(new Object[]{"TINYTEXT", "TINYTEXT", "aa"});
        builder.add(new Object[]{"MEDIUMTEXT", "MEDIUMTEXT", "aa"});
        builder.add(new Object[]{"LONGTEXT", "LONGTEXT", "aa"});
        builder.add(new Object[]{"JSON", "JSON", JsonUtil.toJson(new HashMap<>())});
        builder.add(new Object[]{"GEOMETRY", "GEOMETRY", null});
        builder.add(new Object[]{"ENUM", "ENUM('value1','value2')", null});
        builder.add(new Object[]{"SET", "SET('value1','value2')", null});

        ImmutableList<Object[]> immutableList = builder.build();
        return immutableList;
    }

    @ToString
    static class TestResult{
        RowSet queryResult;
        StringMysqlCollector collector;

        public TestResult(RowSet queryResult, StringMysqlCollector collector) {
            this.queryResult = queryResult;
            this.collector = collector;
        }
        public static TestResult of(RowSet queryResult, StringMysqlCollector collector){
            return new TestResult(queryResult,collector);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestResult that = (TestResult) o;
            boolean equals1 = Objects.equals(queryResult, that.queryResult);
            boolean equals2 = Objects.equals(collector, that.collector);
            return equals1 &&equals2 ;
        }

        @Override
        public int hashCode() {
            return Objects.hash(queryResult, collector);
        }
    }


    static class StringMysqlCollector implements MysqlCollector {
        MycatRowMetaData mycatRowMetaData;
        List<Object[]> objects = new ArrayList<>();
        CountDownLatch countDownLatch = new CountDownLatch(1);

        @Override
        public void onColumnDef(MycatRowMetaData mycatRowMetaData) {
            this.mycatRowMetaData = mycatRowMetaData;
        }

        @Override
        public void onRow(Object[] row) {
            objects.add(row);
        }

        @Override
        public void onComplete() {
            countDownLatch.countDown();
        }

        @Override
        public void onError(Throwable e) {
            countDownLatch.countDown();
        }

        @SneakyThrows
        public void await() {
            countDownLatch.await();
        }

        @Override
        public String toString() {
            StringJoiner joiner = new StringJoiner("\n");
            for (Object[] i : objects) {
                String s;

                List<String> rowString = new ArrayList<>();
                for (Object o : i) {
                    if (o instanceof byte[]) {
                        o = new String((byte[])o);
                    }
                    rowString.add(Objects.toString(o));
                }
                s = rowString.toString();


                joiner.add(s);
            }
            return "StringMysqlCollector{" +
                    "mycatRowMetaData=" + mycatRowMetaData +
                    ", objects=" + joiner +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StringMysqlCollector that = (StringMysqlCollector) o;
            return Objects.equals(mycatRowMetaData, that.mycatRowMetaData) && Objects.equals(objects, that.objects);
        }

        @Override
        public int hashCode() {
            return Objects.hash(mycatRowMetaData, objects);
        }
    }
}
