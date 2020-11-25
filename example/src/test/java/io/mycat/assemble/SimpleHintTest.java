package io.mycat.assemble;

import java.sql.Connection;
import java.sql.SQLException;

public class SimpleHintTest extends AssembleTest {
    @Override
    protected void initCluster(Connection mycatConnection) throws SQLException {
        execute(mycatConnection,
               "/*+ mycat:addDatasource{\"name\":\"dw0\",\"url\":\"jdbc:mysql://127.0.0.1:3306\",\"user\":\"root\",\"password\":\"123456\"} */;");
        execute(mycatConnection,
                "/*+ mycat:addDatasource{\"name\":\"dr0\",\"url\":\"jdbc:mysql://127.0.0.1:3306\",\"user\":\"root\",\"password\":\"123456\"} */;");
        execute(mycatConnection,
                "/*+ mycat:addDatasource{\"name\":\"dw1\",\"url\":\"jdbc:mysql://127.0.0.1:3307\",\"user\":\"root\",\"password\":\"123456\"} */;");
        execute(mycatConnection,
                "/*+ mycat:addDatasource{\"name\":\"dr1\",\"url\":\"jdbc:mysql://127.0.0.1:3307\",\"user\":\"root\",\"password\":\"123456\"} */;");

        execute(mycatConnection,
               "/*! mycat:addCluster{\"name\":\"c0\",\"masters\":[\"dw0\"],\"replicas\":[\"dr0\"]} */;");
        execute(mycatConnection,
                "/*! mycat:addCluster{\"name\":\"c1\",\"masters\":[\"dw1\"],\"replicas\":[\"dr1\"]} */;");
    }
}
