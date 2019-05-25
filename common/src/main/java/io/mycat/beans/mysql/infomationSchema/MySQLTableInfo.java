package io.mycat.beans.mysql.infomationSchema;

import java.sql.Date;

/**
 * @author jamie12221
 *  date 2019-05-16 16:38
 **/
public class MySQLTableInfo {

  String tableCatalog;
  String tableSchema;
  String tableName;
  String tableType;
  String engine;
  int version;
  String rowFormat;
  String tablesRows;
  int avrRowLength;
  String dataLength;
  int maxDataLength;
  int indexLength;
  int dataFree;
  long autoIncrement;
  Date createTime;
  Date updateTime;
  Date checkTime;
  String tableCollation;
  long checkSum;
  String createOptions;
  String tableComment;

  public String getTableCatalog() {
    return tableCatalog;
  }

  public void setTableCatalog(String tableCatalog) {
    this.tableCatalog = tableCatalog;
  }

  public String getTableSchema() {
    return tableSchema;
  }

  public void setTableSchema(String tableSchema) {
    this.tableSchema = tableSchema;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getTableType() {
    return tableType;
  }

  public void setTableType(String tableType) {
    this.tableType = tableType;
  }

  public String getEngine() {
    return engine;
  }

  public void setEngine(String engine) {
    this.engine = engine;
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  public String getRowFormat() {
    return rowFormat;
  }

  public void setRowFormat(String rowFormat) {
    this.rowFormat = rowFormat;
  }

  public String getTablesRows() {
    return tablesRows;
  }

  public void setTablesRows(String tablesRows) {
    this.tablesRows = tablesRows;
  }

  public int getAvrRowLength() {
    return avrRowLength;
  }

  public void setAvrRowLength(int avrRowLength) {
    this.avrRowLength = avrRowLength;
  }

  public String getDataLength() {
    return dataLength;
  }

  public void setDataLength(String dataLength) {
    this.dataLength = dataLength;
  }

  public int getMaxDataLength() {
    return maxDataLength;
  }

  public void setMaxDataLength(int maxDataLength) {
    this.maxDataLength = maxDataLength;
  }

  public int getIndexLength() {
    return indexLength;
  }

  public void setIndexLength(int indexLength) {
    this.indexLength = indexLength;
  }

  public int getDataFree() {
    return dataFree;
  }

  public void setDataFree(int dataFree) {
    this.dataFree = dataFree;
  }

  public long getAutoIncrement() {
    return autoIncrement;
  }

  public void setAutoIncrement(long autoIncrement) {
    this.autoIncrement = autoIncrement;
  }

  public Date getCreateTime() {
    return createTime;
  }

  public void setCreateTime(Date createTime) {
    this.createTime = createTime;
  }

  public Date getUpdateTime() {
    return updateTime;
  }

  public void setUpdateTime(Date updateTime) {
    this.updateTime = updateTime;
  }

  public Date getCheckTime() {
    return checkTime;
  }

  public void setCheckTime(Date checkTime) {
    this.checkTime = checkTime;
  }

  public String getTableCollation() {
    return tableCollation;
  }

  public void setTableCollation(String tableCollation) {
    this.tableCollation = tableCollation;
  }

  public long getCheckSum() {
    return checkSum;
  }

  public void setCheckSum(long checkSum) {
    this.checkSum = checkSum;
  }

  public String getCreateOptions() {
    return createOptions;
  }

  public void setCreateOptions(String createOptions) {
    this.createOptions = createOptions;
  }

  public String getTableComment() {
    return tableComment;
  }

  public void setTableComment(String tableComment) {
    this.tableComment = tableComment;
  }
}
