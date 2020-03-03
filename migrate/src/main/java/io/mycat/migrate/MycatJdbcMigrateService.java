package io.mycat.migrate;

import com.alibaba.druid.pool.DruidDataSource;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.*;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * data migrate service. jdbc operation read and write.
 * @author : wangzihaogithub Date : 2020-03-03 14:03
 */
@Slf4j
public class MycatJdbcMigrateService implements MycatMigrateService{
    @Override
    public void onlineTransfer(TransferRequest request) {
        throw new UnsupportedOperationException("public void onlineTransfer(TransferRequest request)");
    }

    @Override
    public void offlineTransfer(TransferRequest request) {
        String readTableName = request.getReadDataNode().getTableName();
        DataNode readDataNode = request.getReadDataNode();
        DataNode writerDataNode = request.getWriteDataNode();
        Consumer<TransferEvent> eventCallback = request.getTransferEventCallback();
        if(eventCallback == null){
            eventCallback = e ->{};
        }
        int bufferSize = request.getBufferSize();
        boolean skipWriteErrorAllFlag = request.isSkipWriteErrorAllFlag();
        List<Class<SQLException>> skipErrorClassList = Stream.of(request.getSkipWriteErrorClassNames())
                .filter(Objects::nonNull)
                .map(MycatJdbcMigrateService::forNameIfNull)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        int columnCount;
        String readCatalogName;
        Connection readConnection;
        try {
            DataSource readDataSource = newDataSource(readDataNode);
            readConnection = readDataSource.getConnection();
        } catch (SQLException e) {
            String message = MessageFormat.format(MESSAGE_CONNECTION_OPEN_ERROR,readTableName, e.toString());
            log.error(message,e);
            eventCallback.accept(new TransferEvent(EVENT_CONNECTION_OPEN_ERROR,message,0, Collections.singletonList(e),null));
            return;
        }

        try(ResultSet checkResultSet = selectTableAsStream(readConnection, readTableName)){
            ResultSetMetaData checkMetaData = checkResultSet.getMetaData();
            if (checkMetaData.getColumnCount() == 0) {
                String message = MessageFormat.format(MESSAGE_COLUMN_EMPTY_WARN,readTableName, readConnection);
                log.warn(message);
                eventCallback.accept(new TransferEvent(EVENT_COLUMN_EMPTY_WARN,message,0,Collections.emptyList(),null));
                return;
            }
            columnCount = checkMetaData.getColumnCount();
            readCatalogName = checkMetaData.getCatalogName(1);
            if (!checkResultSet.next()) {
                String message = MessageFormat.format(MESSAGE_TABLE_EMPTY_WARN,readTableName,readConnection);
                log.warn(message);
                eventCallback.accept(new TransferEvent(EVENT_TABLE_EMPTY_WARN,message,0,Collections.emptyList(),null));
                return;
            }
        }catch (SQLException e){
            String message = MessageFormat.format(MESSAGE_METADATA_READ_ERROR,
                    readTableName, readConnection,e.toString());
            log.error(message,e);
            eventCallback.accept(new TransferEvent(EVENT_METADATA_READ_ERROR,message,0, Collections.singletonList(e),null));
            return;
        }

        List<String> pkColumnNameList;
        try {
            pkColumnNameList = getPkColumnNameList(readConnection,readCatalogName,readTableName);
        } catch (SQLException e) {
            String message = MessageFormat.format(MESSAGE_PRIMARYKEY_READ_ERROR,
                    readCatalogName, readTableName, readConnection,e.toString());
            log.error(message,e);
            eventCallback.accept(new TransferEvent(EVENT_PRIMARYKEY_READ_ERROR,message,0, Collections.singletonList(e),null));
            return;
        }

        List<SQLException> exceptionList = new LinkedList<>();
        List<RowData> rowDataList = new ArrayList<>(bufferSize);
        long totalWriteCount = 0;
        try(ResultSet readResultSet = selectTableAsStream(readConnection, readTableName)){
            ResultSetMetaData readMetaData = readResultSet.getMetaData();
            while (true){
                boolean next = readResultSet.next();
                int unWriteCount = rowDataList.size();
                if(!next || (unWriteCount > 0 && unWriteCount % bufferSize == 0)){
                    eventCallback.accept(new TransferEvent(EVENT_TRANSFER_WRITE_BEFORE_INFO, MESSAGE_TRANSFER_WRITE_BEFORE_INFO,totalWriteCount,exceptionList,rowDataList));

                    DataSource writerDataSource = newDataSource(writerDataNode);
                    Connection writerConnection = writerDataSource.getConnection();
                    writerConnection.setAutoCommit(false);
                    try {
                        write(rowDataList,writerConnection);
                        writerConnection.commit();
                        totalWriteCount += unWriteCount;
                        rowDataList.clear();
                    }catch (SQLException e){
                        exceptionList.add(e);
                        try {
                            onSQLException(skipWriteErrorAllFlag, e, skipErrorClassList);
                        }catch (SQLException stopWriteSQLException){
                            writerConnection.rollback();
                            String message = MessageFormat.format(MESSAGE_TABLE_WRITE_ERROR,
                                    readCatalogName, readTableName, readConnection,
                                    totalWriteCount, rowDataList.size(), e.toString());
                            log.error(message,e);
                            eventCallback.accept(new TransferEvent(EVENT_TABLE_WRITE_ERROR, message,totalWriteCount,exceptionList,rowDataList));
                            return;
                        }
                    }
                }
                if(!next){
                    break;
                }
                RowData rowData = new RowData();
                rowData.setCatalogName(readCatalogName);
                rowData.setTableName(readTableName);
                rowData.setColumnDatas(new ColumnData[columnCount]);
                for (int i = 1; i <= columnCount; i++) {
                    Object columnValue = readResultSet.getObject(i);
                    ColumnData columnData = new ColumnData();
                    columnData.setPrimaryKey(pkColumnNameList.contains(columnData.getColumnName()));
                    columnData.setColumnClassName(readMetaData.getColumnClassName(i));
                    columnData.setColumnTypeId(readMetaData.getColumnType(i));
                    columnData.setColumnName(readMetaData.getColumnName(i));
                    columnData.setColumnValue(columnValue);
                    rowData.getColumnDatas()[i-1] = columnData;
                }
                rowDataList.add(rowData);
            }
            eventCallback.accept(new TransferEvent(EVENT_SUCCESSFUL_INFO, MESSAGE_SUCCESSFUL_INFO,totalWriteCount,exceptionList,rowDataList));
        }catch (SQLException e){
            String message = MessageFormat.format(MESSAGE_TABLE_READ_ERROR,
                    readCatalogName, readTableName, readConnection,
                    totalWriteCount, rowDataList.size(), e.toString());
            log.error(message,e);
            exceptionList.add(e);
            eventCallback.accept(new TransferEvent(EVENT_TABLE_READ_ERROR,message,totalWriteCount,exceptionList,rowDataList));
        }

//        Iterable<Map<String, List<String>>> maps = MetadataManager.routeInsert(null, null);
    }

    private ResultSet selectTableAsStream(Connection connection, String tableName) throws SQLException {
        PreparedStatement ps = connection.prepareStatement("SELECT * FROM " + tableName,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        ps.setFetchSize(Integer.MIN_VALUE);
        ps.setFetchDirection(ResultSet.FETCH_REVERSE);
        return ps.executeQuery();
    }

    private void onSQLException(boolean skipAllErrorFlag, SQLException exception, List<Class<SQLException>> skipErrorClassList) throws SQLException {
        if(skipAllErrorFlag){
            log.debug("Mycat migrate skipErrorFlag. {}",exception.toString());
        }else {
            boolean isThrows = true;
            for (Class<SQLException> sqlExceptionClass : skipErrorClassList) {
                if(sqlExceptionClass.isAssignableFrom(exception.getClass())){
                    isThrows = false;
                    break;
                }
            }
            if(isThrows){
                throw exception;
            }else {
                log.debug("Mycat migrate skipErrorClass. {}",exception.toString());
            }
        }
    }

    private static void write(List<RowData> rowDataList, Connection connection) throws SQLException {
        //根据库聚合提交
        Map<String, List<RowData>> groupByCatalogNameMap = rowDataList.stream()
                .collect(Collectors.groupingBy(RowData::getCatalogName, Collectors.toList()));
        for (Map.Entry<String, List<RowData>> catalogEntry : groupByCatalogNameMap.entrySet()) {
            List<RowData> catalogRowDataList = catalogEntry.getValue();
            Map<String, List<RowData>> groupByTableNameMap = catalogRowDataList.stream()
                    .collect(Collectors.groupingBy(RowData::getTableName, Collectors.toList()));
            //根据表聚合提交
            for (Map.Entry<String, List<RowData>> tableEntry : groupByTableNameMap.entrySet()) {
                List<RowData> tableRowDataList = tableEntry.getValue();
                if(tableRowDataList.isEmpty()){
                    continue;
                }
                RowData firstRowData = tableRowDataList.get(0);
                String prepareSql = buildPrepareSql(firstRowData);
                PreparedStatement ps = connection.prepareStatement(prepareSql);
                for (RowData rowData : tableRowDataList) {
                    ColumnData[] columnDatas = rowData.getColumnDatas();
                    for (int i = 1; i <= columnDatas.length; i++) {
                        ColumnData columnData = columnDatas[i-1];
                        ps.setObject(i,columnData.getColumnValue(),columnData.getColumnTypeId());
                    }
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        }
    }

    private static String buildPrepareSql(RowData rowData){
        ColumnData[] columnDatas = rowData.getColumnDatas();
        if(columnDatas.length == 0){
            return null;
        }
        String catalogName = rowData.getCatalogName();
        String tableName = rowData.getTableName();
        StringJoiner valuesJoiner = new StringJoiner(",", "values (",")");
        StringJoiner columnJoiner = new StringJoiner(",","insert into "+catalogName+'.'+tableName+" (",")");
        for (ColumnData columnData : columnDatas) {
            columnJoiner.add(columnData.getColumnName());
            valuesJoiner.add("?");
        }
        return columnJoiner.toString().concat(valuesJoiner.toString());
    }

    private static DataSource newDataSource(DataNode dataNode){
        DruidDataSource datasource = new DruidDataSource();
        datasource.setPassword(dataNode.getPassword());
        datasource.setUsername(dataNode.getUsername());
        datasource.setUrl(dataNode.getUrl());
        datasource.setDriverClassName(dataNode.getDriverClassName());
        datasource.setMaxWait(TimeUnit.SECONDS.toMillis(1));
        datasource.setMaxActive(3);
        datasource.setMinIdle(1);
        return datasource;
    }

    @SuppressWarnings("unchecked")
    private static <T extends SQLException>Class<T> forNameIfNull(String className){
        try {
            return (Class<T>) Class.forName(className);
        } catch (ClassNotFoundException e) {
            return null;
        }
    }

    private static List<String> getPkColumnNameList(Connection connection, String catalogName, String tableName) throws SQLException {
        List<String> pkList = new ArrayList<>();
        try(ResultSet resultSet = connection.getMetaData().getPrimaryKeys(catalogName, catalogName, tableName)){
            ResultSetMetaData md = resultSet.getMetaData();
            int primaryKeyColumnCount = md.getColumnCount();
            while (resultSet.next()){
                for (int i = 1; i <= primaryKeyColumnCount; i++) {
                    String columnName = md.getColumnName(i);
                    if(Objects.equals("COLUMN_NAME",columnName)){
                        pkList.add(resultSet.getString(i));
                    }
                }
            }
        }
        return pkList;
    }


    public static void main(String[] args) {
        TransferRequest request = new TransferRequest();
        request.setReadDataNode(new DataNode("jdbc:mysql://localhost:3306/db1?autoReconnectForPools=true&autoReconnect=true&useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai",
                "root","root","company",
                "com.mysql.cj.jdbc.Driver"));
        request.setWriteDataNode(new DataNode("jdbc:mysql://localhost:3306/db2?autoReconnectForPools=true&autoReconnect=true&useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai",
                "root","root","company",
                "com.mysql.cj.jdbc.Driver"));
        request.setTransferEventCallback(event-> {
            switch (event.getEvent()){
                case EVENT_SUCCESSFUL_INFO:
                case EVENT_TRANSFER_WRITE_BEFORE_INFO:
                case EVENT_TABLE_EMPTY_WARN:
                case EVENT_COLUMN_EMPTY_WARN:
                case EVENT_CONNECTION_OPEN_ERROR:
                case EVENT_METADATA_READ_ERROR:
                case EVENT_PRIMARYKEY_READ_ERROR:
                case EVENT_TABLE_READ_ERROR:
                case EVENT_TABLE_WRITE_ERROR:
                default:{
                    System.out.println("message = " + event.getMessage());
                    break;
                }
            }
            for (SQLException sqlException : event.getExceptionList()) {
                System.out.println("sqlException = " + sqlException);
            }
            for (RowData rowData : event.getRowDataList()) {
                System.out.println("rowData = " + rowData + ";");
            }
        });

        MycatMigrateService service = new MycatJdbcMigrateService();
        service.offlineTransfer(request);
        System.out.println("service = " + service);
    }

}
