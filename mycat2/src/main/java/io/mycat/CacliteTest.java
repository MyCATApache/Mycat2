package io.mycat;

import io.mycat.calcite.MetadataManager;
import io.mycat.datasource.jdbc.GRuntime;
import io.mycat.datasource.jdbc.resultset.JdbcRowBaseIteratorImpl;
import io.mycat.replica.ReplicaSelectorRuntime;
import org.apache.calcite.jdbc.CalciteConnection;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

public class CacliteTest {
    public static void main(String[] args) {
        ReplicaSelectorRuntime.INSTCANE.load();
        GRuntime.INSTACNE.load(ConfigRuntime.INSTCANE.load());
        MetadataManager insatnce = MetadataManager.INSATNCE;
        try {
            CalciteConnection connection = insatnce.getConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from TESTDB.TRAVELRECORD limit 2");
            JdbcRowBaseIteratorImpl jdbcRowBaseIterator = new JdbcRowBaseIteratorImpl(statement, resultSet);
            List<Map<String, Object>> resultSetMap = jdbcRowBaseIterator.getResultSetMap();
            for (Map<String, Object> map : resultSetMap) {
                System.out.println(map);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}