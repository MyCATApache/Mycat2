package io.mycat.migrate;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.SQLException;
import java.util.List;
import java.util.StringJoiner;
import java.util.function.Consumer;

/**
 * data migrate service. read and write.
 * @author : wangzihaogithub Date : 2020-03-03 14:02
 */
public interface MycatMigrateService {
    int EVENT_TRANSFER_WRITE_BEFORE_INFO = 1001;
    String MESSAGE_TRANSFER_WRITE_BEFORE_INFO = "Mycat migrate info#1001. write before\n";
    int EVENT_SUCCESSFUL_INFO = 2000;
    String MESSAGE_SUCCESSFUL_INFO = "Mycat migrate info#2000. successful\n";
    int EVENT_TABLE_EMPTY_WARN = 4000;
    String MESSAGE_TABLE_EMPTY_WARN = "Mycat migrate warn#4000. a empty table. tableName={0},readConnection={1}\n";
    int EVENT_COLUMN_EMPTY_WARN = 4001;
    String MESSAGE_COLUMN_EMPTY_WARN = "Mycat migrate warn#4001. a empty column. tableName={0},readConnection={1}\n";
    int EVENT_CONNECTION_OPEN_ERROR = 5000;
    String MESSAGE_CONNECTION_OPEN_ERROR = "Mycat migrate error#5000. connection open failure. tableName={0},error={1}\n";
    int EVENT_METADATA_READ_ERROR = 5001;
    String MESSAGE_METADATA_READ_ERROR = "Mycat migrate error#5001. check metaData failure. tableName={0},readConnection={1},error={2}\n";
    int EVENT_PRIMARYKEY_READ_ERROR = 5002;
    String MESSAGE_PRIMARYKEY_READ_ERROR = "Mycat migrate error#5002. getPkColumnNameList failure. catalogName={0},tableName={1},readConnection={2},error={3}\n";
    int EVENT_TABLE_READ_ERROR = 5003;
    String MESSAGE_TABLE_READ_ERROR = "Mycat migrate error#5003. select stream handle failure. catalogName={0},tableName={1},readConnection={2},totalWriteCount={3},unWriteCount={4},error={5}\n";
    int EVENT_TABLE_WRITE_ERROR = 5004;
    String MESSAGE_TABLE_WRITE_ERROR = "Mycat migrate error#5004. write data handle failure. catalogName={0},tableName={1},readConnection={2},totalWriteCount={3},unWriteCount={4},error={5}\n";

    /**
     * Offline data transfer. Copy the data node to another node
     * Jdbc is implement = select * from table
     *
     * You can set up a callback request in the event the way to handle logical data replication
     * @param request row data will remain its reference data,
     *                you can modify its,
     *                the result will be written to change. {@link RowData}
     */
    void offlineTransfer(TransferRequest request);

    /**
     * onlineTransfer
     * TODO: 2020-03-03 14:12
     * @param request TransferRequest
     */
    void onlineTransfer(TransferRequest request);

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    class TransferRequest {
        private DataNode readDataNode;
        private DataNode writeDataNode;
        private int bufferSize = 2000;
        private boolean skipWriteErrorAllFlag = false;
        private String[] skipWriteErrorClassNames = {"java.sql.SQLIntegrityConstraintViolationException"};
        private Consumer<TransferEvent> transferEventCallback;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    class TransferEvent {
        private int event = EVENT_SUCCESSFUL_INFO;
        private String message = MESSAGE_SUCCESSFUL_INFO;
        private long writeCompleteCount;
        /**
         * If you skip or abnormal, it will appear here, this collection will remain the order of the exception.
         */
        private List<SQLException> exceptionList;
        /**
         * You can modify rows of data in the event before writing data to achieve the purpose of routing or modifying data
         * {@link #event, #EVENT_TRANSFER_WRITE_BEFORE_INFO}
         */
        private List<RowData> rowDataList;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    class DataNode{
        private String url;
        private String username;
        private String password;
        private String tableName;
        private String driverClassName = "com.mysql.cj.jdbc.Driver";
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    class ColumnData implements Cloneable{
        private boolean primaryKey;
        private int columnTypeId;
        private String columnClassName;
        private String columnName;
        private Object columnValue;
        @Override
        public ColumnData clone(){
            try {
                return (ColumnData) super.clone();
            } catch (CloneNotSupportedException e) {
                return new ColumnData(primaryKey,columnTypeId,columnClassName,columnName,columnValue);
            }
        }
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Data
    class RowData implements Cloneable{
        private String catalogName;
        private String tableName;
        private ColumnData[] columnDatas;
        @Override
        public RowData clone(){
            RowData result;
            try {
                result = (RowData) super.clone();
            } catch (CloneNotSupportedException e) {
                result = new RowData();
            }
            result.tableName = tableName;
            result.catalogName = catalogName;
            result.columnDatas = new ColumnData[columnDatas.length];
            for (int i = 0, size = result.columnDatas.length; i < size; i++) {
                result.columnDatas[i] = columnDatas[i].clone();
            }
            return result;
        }
        @Override
        public String toString() {
            StringJoiner columnJoiner = new StringJoiner(",","insert into "+catalogName+'.'+tableName+" (",")");
            StringJoiner valuesJoiner = new StringJoiner(",", " values (",")");
            for (ColumnData columnData : columnDatas) {
                columnJoiner.add(columnData.getColumnName());
                Object columnValue = columnData.getColumnValue();
                if(columnValue == null || columnValue instanceof Number){
                    valuesJoiner.add(String.valueOf(columnValue));
                }else {
                    valuesJoiner.add("'"+ columnValue +"'");
                }
            }
            return columnJoiner.toString().concat(valuesJoiner.toString());
        }
    }

}
