package io.mycat;

import com.mysql.cj.jdbc.MysqlDataSource;
import lombok.SneakyThrows;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.Optional;
import java.util.Properties;

public class MetaDataConfig {

    @SneakyThrows
    public static void main(String[] args) {
        String username = "root";
        String password = "123456";

        Properties properties = new Properties();
        properties.put("user", username);
        properties.put("password", password);
        properties.put("useBatchMultiSend", "false");
        properties.put("usePipelineAuth", "false");

        String url = "jdbc:mysql://localhost.1:3306/db1?useServerPrepStmts=false&useCursorFetch=true&serverTimezone=UTC&allowMultiQueries=false&useBatchMultiSend=false&characterEncoding=utf8";

        MysqlDataSource mysqlDataSource = new MysqlDataSource();
        mysqlDataSource.setUrl(url);
        mysqlDataSource.setUser(username);
        mysqlDataSource.setPassword(password);

        try (Connection connection = DriverManager.getConnection(url, properties)) {
            try (Statement statement = connection.createStatement()) {
                ResultSet resultSet = statement.executeQuery("SELECT * FROM information_schema.COLUMNS  ");
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();
                resultSet.next();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    JDBCType jdbcType = JDBCType.valueOf(metaData.getColumnType(i));
                    Object object = null;
                    try {
                        object = resultSet.getObject(i);
                    } catch (Exception e) {

                    }
                    Class<?> aClass = null;
                    if (object != null) {
                        aClass = object.getClass();
                    }
                    System.out.println("" + Optional.ofNullable(aClass).map(j -> j.getSimpleName()).orElse(jdbcType.getName()) + " " + columnName + " ;");
                }
            }
        }
    }

    public static class TABLE_CONSTRAINTS {
        String CONSTRAINT_CATALOG;
        String CONSTRAINT_SCHEMA;
        String CONSTRAINT_NAME;
        String TABLE_SCHEMA;
        String TABLE_NAME;
        String CONSTRAINT_TYPE;
    }

    public static class VIEWS {
        String TABLE_CATALOG;
        String TABLE_SCHEMA;
        String TABLE_NAME;
        String VIEW_DEFINITION;
        String CHECK_OPTION;
        String IS_UPDATABLE;
        String DEFINER;
        String SECURITY_TYPE;
        String CHARACTER_SET_CLIENT;
        String COLLATION_CONNECTION;
        String ALGORITHM;
    }

    public static class TABLES {
        String TABLE_CATALOG;
        String TABLE_SCHEMA;
        String TABLE_NAME;
        String TABLE_TYPE;
        String ENGINE;
        BigInteger VERSION;
        String ROW_FORMAT;
        Long TABLE_ROWS;
        BigInteger AVG_ROW_LENGTH;
        BigInteger DATA_LENGTH;
        BigInteger MAX_DATA_LENGTH;
        BigInteger INDEX_LENGTH;
        BigInteger DATA_FREE;
        Long AUTO_INCREMENT;
        Timestamp CREATE_TIME;
        Timestamp UPDATE_TIME;
        Timestamp CHECK_TIME;
        String TABLE_COLLATION;
        Long CHECKSUM;
        String CREATE_OPTIONS;
        String TABLE_COMMENT;
        BigInteger MAX_INDEX_LENGTH;
        String TEMPORARY;
    }

    public static class SCHEMATA {
        String CATALOG_NAME;
        String SCHEMA_NAME;
        String DEFAULT_CHARACTER_SET_NAME;
        String DEFAULT_COLLATION_NAME;
        String SQL_PATH;
    }

    public static class STATISTICS {
        String TABLE_CATALOG;
        String TABLE_SCHEMA;
        String TABLE_NAME;
        Long NON_UNIQUE;
        String INDEX_SCHEMA;
        String INDEX_NAME;
        Long SEQ_IN_INDEX;
        String COLUMN_NAME;
        String COLLATION;
        Long CARDINALITY;
        Long SUB_PART;
        Long PACKED;
        String NULLABLE;
        String INDEX_TYPE;
        String COMMENT;
        String INDEX_COMMENT;
    }

    public static class COLLATIONS {
        String COLLATION_NAME;
        String CHARACTER_SET_NAME;
        Long ID;
        String IS_DEFAULT;
        String IS_COMPILED;
        Long SORTLEN;
    }

    public static class PROCESSLIST {
        Long ID;
        String USER;
        String HOST;
        String DB;
        String COMMAND;
        Integer TIME;
        String STATE;
        String INFO;
        BigDecimal TIME_MS;
        Integer STAGE;
        Integer MAX_STAGE;
        BigDecimal PROGRESS;
        Long MEMORY_USED;
        Long MAX_MEMORY_USED;
        Integer EXAMINED_ROWS;
        Long QUERY_ID;
        byte[] INFO_BINARY;
        Long TID;
    }

    public static class COLLATION_CHARACTER_SET_APPLICABILITY {
        String COLLATION_NAME;
        String CHARACTER_SET_NAME;

    }

    public static class KEY_COLUMN_USAGE {
        String CONSTRAINT_CATALOG;
        String CONSTRAINT_SCHEMA;
        String CONSTRAINT_NAME;
        String TABLE_CATALOG;
        String TABLE_SCHEMA;
        String TABLE_NAME;
        String COLUMN_NAME;
        Long ORDINAL_POSITION;
        Long POSITION_IN_UNIQUE_CONSTRAINT;
        String REFERENCED_TABLE_SCHEMA;
        String REFERENCED_TABLE_NAME;
        String REFERENCED_COLUMN_NAME;
    }

    public static class CHARACTER_SETS {
        String CHARACTER_SET_NAME;
        String DEFAULT_COLLATE_NAME;
        String DESCRIPTION;
        Long MAXLEN;
    }

    public static class COLUMNS {
        String TABLE_CATALOG;
        String TABLE_SCHEMA;
        String TABLE_NAME;
        String COLUMN_NAME;
        BigInteger ORDINAL_POSITION;
        String COLUMN_DEFAULT;
        String IS_NULLABLE;
        String DATA_TYPE;
        BigInteger CHARACTER_MAXIMUM_LENGTH;
        BigInteger CHARACTER_OCTET_LENGTH;
        Long NUMERIC_PRECISION;
        Long NUMERIC_SCALE;
        Long DATETIME_PRECISION;
        String CHARACTER_SET_NAME;
        String COLLATION_NAME;
        String COLUMN_TYPE;
        String COLUMN_KEY;
        String EXTRA;
        String PRIVILEGES;
        String COLUMN_COMMENT;
        String IS_GENERATED;
        String GENERATION_EXPRESSION;
    }
}