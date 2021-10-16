package io.mycat;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateViewStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.beans.mysql.MySQLType;
import io.mycat.config.NormalBackEndTableInfoConfig;
import io.mycat.config.NormalTableConfig;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.util.CalciteConvertors;
import io.mycat.util.MycatSQLExprTableSourceUtil;
import io.mycat.util.SQL2ResultSetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

public class PrototypeService {
    public final static String PROTOTYPE = "prototype";
    private static final Logger LOGGER = LoggerFactory.getLogger(PrototypeService.class);

    public PrototypeService() {

    }

    public RowBaseIterator handleSql(SQLStatement statement) {
        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
        return resultSetBuilder.build();
    }

    public Optional<JdbcConnectionManager> getPrototypeConnectionManager() {
        if (MetaClusterCurrent.exist(JdbcConnectionManager.class)) {
            return Optional.of(MetaClusterCurrent.wrapper(JdbcConnectionManager.class));
        }
        return Optional.empty();
    }


    public Optional<String> getCreateTableSQLByJDBC(String schemaName, String tableName, List<Partition> backends) {
        Optional<JdbcConnectionManager> jdbcConnectionManagerOptional = getPrototypeConnectionManager();
        if (!jdbcConnectionManagerOptional.isPresent()) {
            return Optional.empty();
        }
        JdbcConnectionManager jdbcConnectionManager = jdbcConnectionManagerOptional.get();
        backends = new ArrayList<>(backends);
        backends.add(new BackendTableInfo(PROTOTYPE, schemaName, tableName));

        if (backends == null || backends.isEmpty()) {
            return null;
        }
        for (Partition backend : backends) {
            try {
                Partition backendTableInfo = backend;
                String targetName = backendTableInfo.getTargetName();
                String targetSchemaTable = backendTableInfo.getTargetSchemaTable();
                try (DefaultConnection connection = jdbcConnectionManager.getConnection(targetName)) {
                    String sql = "SHOW CREATE TABLE " + targetSchemaTable;
                    try (RowBaseIterator rowBaseIterator = connection.executeQuery(sql)) {
                        while (rowBaseIterator.next()) {
                            String string = rowBaseIterator.getString(1);
                            SQLStatement sqlStatement = null;
                            try {
                                sqlStatement = SQLUtils.parseSingleMysqlStatement(string);
                            } catch (Throwable e) {

                            }
                            if (sqlStatement == null) {
                                try {
                                    string = string.substring(0, string.lastIndexOf(')') + 1);
                                    sqlStatement = SQLUtils.parseSingleMysqlStatement(string);
                                } catch (Throwable e) {

                                }
                            }
                            if (sqlStatement instanceof MySqlCreateTableStatement) {
                                MySqlCreateTableStatement sqlStatement1 = (MySqlCreateTableStatement) sqlStatement;

                                sqlStatement1.setTableName(SQLUtils.normalize(tableName));
                                sqlStatement1.setSchema(SQLUtils.normalize(schemaName));//顺序不能颠倒
                                return Optional.of(sqlStatement1.toString());
                            }
                            if (sqlStatement instanceof SQLCreateViewStatement) {
                                SQLCreateViewStatement sqlStatement1 = (SQLCreateViewStatement) sqlStatement;
                                SQLExprTableSource sqlExprTableSource = sqlStatement1.getTableSource();
                                if (!SQLUtils.nameEquals(sqlExprTableSource.getTableName(), tableName) ||
                                        !SQLUtils.nameEquals(sqlExprTableSource.getSchema(), (schemaName))) {
                                    MycatSQLExprTableSourceUtil.setSqlExprTableSource(schemaName, tableName, sqlExprTableSource);
                                    return Optional.of(sqlStatement1.toString());
                                } else {
                                    return Optional.of(string);
                                }
                            }

                        }
                    } catch (Exception e) {
                        LOGGER.error("", e);
                    }
                    try (RowBaseIterator rowBaseIterator = connection.executeQuery("select * from " + targetSchemaTable + " where 0 limit 0")) {
                        MycatRowMetaData metaData = rowBaseIterator.getMetaData();
                        MySqlCreateTableStatement mySqlCreateTableStatement = new MySqlCreateTableStatement();
                        mySqlCreateTableStatement.setTableName(tableName);
                        mySqlCreateTableStatement.setSchema(schemaName);
                        int columnCount = metaData.getColumnCount();
                        for (int i = 0; i < columnCount; i++) {
                            int columnType = metaData.getColumnType(i);
                            String type = SQLDataType.Constants.VARCHAR;
                            for (MySQLType value : MySQLType.values()) {
                                if (value.getJdbcType() == columnType) {
                                    type = value.getName();
                                }
                            }
                            mySqlCreateTableStatement.addColumn(metaData.getColumnName(i), type);
                        }
                        return Optional.of(mySqlCreateTableStatement.toString());

                    }
                }
            } catch (Throwable e) {
                LOGGER.error("can not get create table sql from:" + backend.getTargetName() + backend.getTargetSchemaTable(), e);
                continue;
            }
        }
        return Optional.empty();
    }

    public List<SimpleColumnInfo> getColumnInfo(String sql) {
        String prototypeServer = PROTOTYPE;
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
        MycatRowMetaData mycatRowMetaData = null;
        if (sqlStatement instanceof MySqlCreateTableStatement) {
            mycatRowMetaData = SQL2ResultSetUtil.getMycatRowMetaData((MySqlCreateTableStatement) sqlStatement);
        }
        if (sqlStatement instanceof SQLCreateViewStatement) {
            Optional<JdbcConnectionManager> prototypeConnectionManagerOptional = getPrototypeConnectionManager();
            if (!prototypeConnectionManagerOptional.isPresent()) return Collections.emptyList();
            mycatRowMetaData = SQL2ResultSetUtil.getMycatRowMetaData(prototypeConnectionManagerOptional.get(), prototypeServer, (SQLCreateViewStatement) sqlStatement);
        }
        return CalciteConvertors.getColumnInfo(Objects.requireNonNull(mycatRowMetaData));
    }


    public Map<String, NormalTableConfig> getDefaultNormalTable(String targetName, String schemaName, Predicate<String> tableFilter) {
        Set<String> tables = new HashSet<>();
        Optional<JdbcConnectionManager> jdbcConnectionManagerOptional = getPrototypeConnectionManager();
        if (!jdbcConnectionManagerOptional.isPresent()) {
            return Collections.emptyMap();
        }
        try (DefaultConnection connection = jdbcConnectionManagerOptional.get().getConnection(targetName)) {
            RowBaseIterator tableIterator = connection.executeQuery("show tables from " + schemaName);
            while (tableIterator.next()) {
                tables.add(tableIterator.getString(0));
            }
        } catch (Exception e) {
            LOGGER.error("", e);
            return Collections.emptyMap();
        }
        Map<String, NormalTableConfig> res = new ConcurrentHashMap<>();
        tables.stream().filter(tableFilter).parallel().forEach(tableName -> {
            NormalBackEndTableInfoConfig normalBackEndTableInfoConfig = new NormalBackEndTableInfoConfig(targetName, schemaName, tableName);
            try {
                String createTableSQLByJDBC = getCreateTableSQLByJDBC(schemaName, tableName,
                        Collections.singletonList(new BackendTableInfo(targetName, schemaName, tableName))).orElse(null);
                if (createTableSQLByJDBC != null) {
                    res.put(tableName, new NormalTableConfig(createTableSQLByJDBC, normalBackEndTableInfoConfig));
                }else {
                    //exception
                }
            } catch (Throwable e) {
                LOGGER.warn("", e);
            }
        });
        return res;
    }
}
