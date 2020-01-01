//package io.mycat;
//
//import io.mycat.calcite.CalciteEnvironment;
//import io.mycat.datasource.jdbc.JdbcRuntime;
//import io.mycat.datasource.jdbc.resultset.JdbcRowBaseIteratorImpl;
//import io.mycat.replica.ReplicaSelectorRuntime;
//import org.apache.calcite.jdbc.CalciteConnection;
//import org.apache.calcite.tools.RelBuilder;
//import org.apache.calcite.tools.RelBuilderFactory;
//
//import java.sql.ResultSet;
//import java.sql.Statement;
//import java.util.List;
//import java.util.Map;
//
//public class CacliteTest {
//    public static void main(String[] args) {
//        ReplicaSelectorRuntime.INSTCANE.load();
//        JdbcRuntime.INSTACNE.load(ConfigRuntime.INSTCANE.load());
//        try {
//            CalciteConnection connection = CalciteEnvironment.INSTANCE.getConnection();
//            Statement statement = connection.createStatement();
//            ResultSet resultSet = statement.executeQuery("select (select count(t.id) from  TESTDB.TRAVELRECORD as t    where t.id not between 1 and 2 and t.user_id = t2.id or " +
//                    "((not exists (select t.user_id from  TESTDB.TRAVELRECORD as t3  where t3.id = 4))) ) from TESTDB.TRAVELRECORD as t2 limit 2");
//            RelBuilderFactory proto = RelBuilder.proto(connection.createPrepareContext().getDataContext());
//            JdbcRowBaseIteratorImpl jdbcRowBaseIterator = new JdbcRowBaseIteratorImpl(statement, resultSet);
//            List<Map<String, Object>> resultSetMap = jdbcRowBaseIterator.getResultSetMap();
//            for (Map<String, Object> map : resultSetMap) {
//                System.out.println(map);
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//    }
//}