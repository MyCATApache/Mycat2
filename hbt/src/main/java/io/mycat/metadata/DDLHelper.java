package io.mycat.metadata;

import io.mycat.DataNode;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;

public class DDLHelper {

    public static void createDatabaseIfNotExist(DefaultConnection connection, DataNode node) {
        createDatabaseIfNotExist(connection, node.getSchema());
    }

    public static void createDatabaseIfNotExist(DefaultConnection connection, String schema) {
        connection.executeUpdate("CREATE DATABASE IF NOT EXISTS " + schema, false);
    }

}
