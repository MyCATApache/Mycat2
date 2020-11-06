package io.mycat.migrate;

import com.alibaba.druid.pool.DruidDataSource;
import lombok.Setter;
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
 *
 * example codes #{@link #main(String[])}
 * @author : wangzihaogithub Date : 2020-03-03 14:03
 */
@Slf4j
public class MycatJdbcMigrateService implements MycatMigrateService {
    @Setter
    private int bufferSize = 2000;

    /**
     * 接口使用示范 {@link #transfer}
     * @param args args
     */
    public static void main(String[] args) {
        TransferRequest request = TransferRequest.builder()
                .readDataNode(DataNode.builder()
                        .url("jdbc:mysql://192.168.101.222:3306/db1?rewriteBatchedStatements=true&characterEncoding=UTF-8&serverTimezone=GMT%2B8")
                        .username("root").password("root").tableName("my_table")
                        .build())
                .writeDataNode(DataNode.builder()
                        .url("jdbc:mysql://192.168.101.189:3306/db1?rewriteBatchedStatements=true&characterEncoding=UTF-8&serverTimezone=GMT%2B8")
                        .username("root").password("root").tableName("my_table_copy")
                        .build())
                //过滤条件
                .readSqlWhere("where id > 1000")
                //自动建表
                .autoCreateTableIfNotExist(true)
                //这里可以加sql重写的逻辑
                .transferEventCallback(MycatJdbcMigrateService::eventHandleExample1)
                .build();

        MycatMigrateService service = new MycatJdbcMigrateService();
        service.transfer(request);
        System.out.println();
    }

    private static void eventHandleExample1(TransferEvent event){
        switch (event.getEvent()){
            //写入前的事件。可扩展自定义逻辑, 比如改库，改表，改数据
            case EVENT_TRANSFER_WRITE_BEFORE_INFO:{
                for (SQLException sqlException : event.getExceptionList()) {
                    System.out.println(sqlException.toString());
                }
                break;
            }
            //执行成功后
            case EVENT_SUCCESSFUL_INFO:
                //空表警告
            case EVENT_TABLE_EMPTY_WARN:
                //空列警告
            case EVENT_COLUMN_EMPTY_WARN:
                //数据库链接打不开
            case EVENT_CONNECTION_OPEN_ERROR:
                //元数据无法读取
            case EVENT_METADATA_READ_ERROR:
                //主键无法读取
            case EVENT_PRIMARYKEY_READ_ERROR:
                //读取数据出错
            case EVENT_TABLE_READ_ERROR:
                //写入数据出错
            case EVENT_TABLE_WRITE_ERROR:
                //建表出错
            case EVENT_TABLE_CREATE_ERROR:
            default:{
                System.out.println(event.getMessage());
                break;
            }
        }
    }

    /**
     * Single-threaded operation, call those who choose their own multi-threaded operating outside the method.
     * @param request row data will remain its reference data,
     *                you can modify its,
     */
    @Override
    public void transfer(TransferRequest request) {
        try (DruidDataSource readDataSource = newDataSource(request.getReadDataNode());
             DruidDataSource writeDataSource = newDataSource(request.getWriteDataNode())){
            //流式传输,一条一条读数据,避免内存撑爆.
            streamTransfer(request,readDataSource,writeDataSource);
        }
    }

    private void streamTransfer(TransferRequest request, DataSource readDataSource, DataSource writeDataSource) {
        //获取参数
        String readSqlWhere = request.getReadSqlWhere();
        DataNode readDataNode = request.getReadDataNode();
        String readTableName = readDataNode.getTableName();
        DataNode writerDataNode = request.getWriteDataNode();
        String writeTableName = writerDataNode.getTableName();
        int maxTransactionSize = request.getMaxTransactionSize();
        Consumer<TransferEvent> eventCallback = request.getTransferEventCallback();
        if(eventCallback == null){
            eventCallback = e ->{};
        }
        List<Class<SQLException>> skipErrorClassList = Stream.of(request.getSkipWriteErrorClassNames())
                .filter(Objects::nonNull)
                .map(this::forNameIfNull)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        //打开读库写库的连接
        Connection readConnection = null;
        Connection writeConnection;
        try {
            readConnection = readDataSource.getConnection();
            writeConnection = writeDataSource.getConnection();
            writeConnection.setAutoCommit(request.isEnableTransaction());
        } catch (SQLException e) {
            readDataNode.setPassword("*");
            writerDataNode.setPassword("*");
            String message = MessageFormat.format(MESSAGE_CONNECTION_OPEN_ERROR,readConnection == null? readDataNode: writerDataNode, getCase(e).toString());
            log.error(message,e);
            eventCallback.accept(new TransferEvent(EVENT_CONNECTION_OPEN_ERROR,message,0, Collections.singletonList(e),null));
            return;
        }

        //先检查一下表中是否有数据
        String readCatalogName = getCatalogAndIfEmptyCallEvent(readConnection,readTableName,readSqlWhere,eventCallback);
        if(readCatalogName == null){
            return;
        }

        //获取主键列
        List<String> pkColumnNameList;
        try {
            pkColumnNameList = getPkColumnNameList(readConnection,readCatalogName,readTableName);
        } catch (SQLException e) {
            String message = MessageFormat.format(MESSAGE_PRIMARYKEY_READ_ERROR,
                    readCatalogName, readTableName, readConnection,getCase(e).toString());
            log.error(message,e);
            eventCallback.accept(new TransferEvent(EVENT_PRIMARYKEY_READ_ERROR,message,0, Collections.singletonList(e),null));
            return;
        }

        //自动建表
        if(request.isAutoCreateTableIfNotExist() && !existTable(writeConnection,writeTableName)){
            String createTableSql = generateCreateTableSql(readConnection, readTableName, writeTableName);
            try {
                writeConnection.createStatement().executeUpdate(createTableSql);
            } catch (SQLException e) {
                String message = MessageFormat.format(MESSAGE_TABLE_CREATE_ERROR,
                        readCatalogName, readTableName, readConnection,createTableSql,getCase(e).toString());
                log.error(message,e);
                eventCallback.accept(new TransferEvent(EVENT_TABLE_CREATE_ERROR,message,0, Collections.singletonList(e),null));
                return;
            }
        }

        //边读边写
        int bufferSize = this.bufferSize;
        List<SQLException> exceptionList = new LinkedList<>();
        List<RowData> rowDataList = new ArrayList<>(bufferSize);
        long totalWriteCount = 0;
        //读取源库数据. 例: select * from table where id > 1000 或 select * from table
        try(ResultSet readResultSet = selectTableAsStream(readConnection, readTableName,readSqlWhere)){
            ResultSetMetaData readMetaData = readResultSet.getMetaData();
            int columnCount = readMetaData.getColumnCount();
            while (true){
                boolean next = readResultSet.next();
                boolean stop = !next;
                int unWriteCount = rowDataList.size();
                //提交数据
                if(stop || (unWriteCount > 0 && unWriteCount % bufferSize == 0)){
                    eventCallback.accept(new TransferEvent(EVENT_TRANSFER_WRITE_BEFORE_INFO, MESSAGE_TRANSFER_WRITE_BEFORE_INFO,totalWriteCount,exceptionList,rowDataList));
                    try {
                        //写入目标库
                        write(rowDataList,writeConnection);
                        totalWriteCount += unWriteCount;
                        //超过了事物上限,或者结束了
                        if(stop || totalWriteCount > maxTransactionSize){
                            writeConnection.commit();
                        }
                        rowDataList.clear();
                        exceptionList.clear();
                    }catch (SQLException e){
                        exceptionList.add(e);
                        try {
                            onSQLException(request.isSkipAllWriteErrorFlag(), e, skipErrorClassList);
                        }catch (SQLException stopWriteSqlException){
                            writeConnection.rollback();
                            String message = MessageFormat.format(MESSAGE_TABLE_WRITE_ERROR,
                                    readCatalogName, readTableName, readConnection,
                                    totalWriteCount, rowDataList.size(), getCase(e).toString());
                            log.error(message,e);
                            eventCallback.accept(new TransferEvent(EVENT_TABLE_WRITE_ERROR, message,totalWriteCount,exceptionList,rowDataList));
                            return;
                        }
                    }
                }
                if(stop){
                    break;
                }
                RowData rowData = new RowData();
                rowData.setCatalogName("");
                rowData.setTableName(writeTableName);
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
            //成功回调
            eventCallback.accept(new TransferEvent(EVENT_SUCCESSFUL_INFO, MESSAGE_SUCCESSFUL_INFO,totalWriteCount,exceptionList,rowDataList));
        }catch (SQLException e){
            //异常回调
            String message = MessageFormat.format(MESSAGE_TABLE_READ_ERROR,
                    readCatalogName, readTableName, readConnection,
                    totalWriteCount, rowDataList.size(), getCase(e).toString());
            log.error(message,e);
            exceptionList.add(e);
            eventCallback.accept(new TransferEvent(EVENT_TABLE_READ_ERROR,message,totalWriteCount,exceptionList,rowDataList));
        }

        log.info("normal stop...");
//        Iterable<Map<String, List<String>>> maps = MetadataManager.routeInsert(null, null);
    }

    private String generateCreateTableSql(Connection connection,String sourceTableName,String targetTableName) {
        int index = 2;
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SHOW CREATE TABLE "+ sourceTableName)){
            if(resultSet.next() && resultSet.getMetaData().getColumnCount() >= index){
                String createTableSql = resultSet.getString(index);
                if(targetTableName != null && targetTableName.length() > 0 && !Objects.equals(sourceTableName,targetTableName)){
                    createTableSql = createTableSql.replaceFirst(sourceTableName,targetTableName);
                }
                return createTableSql;
            }else {
                return null;
            }
        } catch (SQLException e) {
            return null;
        }
    }

    protected boolean existTable(Connection connection,String tableName) {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT * FROM "+ tableName)){
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    protected String getCatalogAndIfEmptyCallEvent(Connection readConnection, String readTableName, String readSqlWhere, Consumer<TransferEvent> eventConsumer){
        try(ResultSet checkResultSet = selectTableAsStream(readConnection, readTableName,readSqlWhere)){
            ResultSetMetaData checkMetaData = checkResultSet.getMetaData();
            if (checkMetaData.getColumnCount() == 0) {
                String message = MessageFormat.format(MESSAGE_COLUMN_EMPTY_WARN,readTableName, readConnection);
                log.warn(message);
                eventConsumer.accept(new TransferEvent(EVENT_COLUMN_EMPTY_WARN,message,0,Collections.emptyList(),null));
                return null;
            }
            String readCatalogName = checkMetaData.getCatalogName(1);
            if (!checkResultSet.next()) {
                String message = MessageFormat.format(MESSAGE_TABLE_EMPTY_WARN,readTableName,readConnection);
                log.warn(message);
                eventConsumer.accept(new TransferEvent(EVENT_TABLE_EMPTY_WARN,message,0,Collections.emptyList(),null));
                return null;
            }
            return readCatalogName;
        }catch (SQLException e){
            String message = MessageFormat.format(MESSAGE_METADATA_READ_ERROR,
                    readTableName, readConnection,getCase(e).toString());
            log.error(message,e);
            eventConsumer.accept(new TransferEvent(EVENT_METADATA_READ_ERROR,message,0, Collections.singletonList(e),null));
            return null;
        }
    }

    protected ResultSet selectTableAsStream(Connection connection, String tableName,String where) throws SQLException {
        PreparedStatement ps = connection.prepareStatement("SELECT * FROM " + tableName +" "+ Objects.toString(where,""),
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        ps.setFetchSize(Integer.MIN_VALUE);
        ps.setFetchDirection(ResultSet.FETCH_REVERSE);
        return ps.executeQuery();
    }

    protected void onSQLException(boolean skipAllErrorFlag, SQLException exception, List<Class<SQLException>> skipErrorClassList) throws SQLException {
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

    protected void write(List<RowData> rowDataList, Connection connection) throws SQLException {
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

    protected String buildPrepareSql(RowData rowData){
        ColumnData[] columnDatas = rowData.getColumnDatas();
        if(columnDatas.length == 0){
            return null;
        }
        String catalogName = rowData.getCatalogName();
        String tableName = rowData.getTableName();
        String target = catalogName == null || catalogName.isEmpty()? tableName: catalogName+'.'+tableName;

        StringJoiner valuesJoiner = new StringJoiner(",", "values (",")");
        StringJoiner columnJoiner = new StringJoiner(",","insert into "+ target +" (",")");
        for (ColumnData columnData : columnDatas) {
            columnJoiner.add(columnData.getColumnName());
            valuesJoiner.add("?");
        }
        return columnJoiner.toString().concat(valuesJoiner.toString());
    }

    protected DruidDataSource newDataSource(DataNode dataNode){
        DruidDataSource datasource = new DruidDataSource();
        datasource.setPassword(dataNode.getPassword());
        datasource.setUsername(dataNode.getUsername());
        datasource.setUrl(dataNode.getUrl());
        datasource.setDriverClassName(dataNode.getDriverClassName());
        datasource.setMaxWait(TimeUnit.SECONDS.toMillis(3));
        datasource.setMaxActive(2);
        datasource.setMinIdle(1);
        return datasource;
    }

    @SuppressWarnings("unchecked")
    protected <T extends SQLException>Class<T> forNameIfNull(String className){
        try {
            return (Class<T>) Class.forName(className);
        } catch (ClassNotFoundException e) {
            return null;
        }
    }

    protected List<String> getPkColumnNameList(Connection connection, String catalogName, String tableName) throws SQLException {
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

    private static Throwable getCase(Throwable exception){
        Throwable prev = exception;
        while (exception != null){
            prev = exception;
            exception = exception.getCause();
        }
        return prev;
    }
}
