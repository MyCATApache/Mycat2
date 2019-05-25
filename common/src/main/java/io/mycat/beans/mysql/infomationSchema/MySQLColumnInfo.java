package io.mycat.beans.mysql.infomationSchema;

/**
 * @author jamie12221
 *  date 2019-05-16 16:50
 **/
public class MySQLColumnInfo {

  String tableCatalog;
  String schemaName;
  String tableName;
  String columnName;
  int ordinalPosition;
  byte[] columnDefault;
  boolean isNullable;
  String dataType;
  Integer characterMaximumLength;
  Integer characterOctetLength;
  Integer numericPrecision;
  Integer numericScale;
  String characterSetName;
  String collationName;
  String columnType;
  String columnKey;
  String extra;
  String privateges;
  String columnComment;

  public String getTableCatalog() {
    return tableCatalog;
  }

  public void setTableCatalog(String tableCatalog) {
    this.tableCatalog = tableCatalog;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getColumnName() {
    return columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public int getOrdinalPosition() {
    return ordinalPosition;
  }

  public void setOrdinalPosition(int ordinalPosition) {
    this.ordinalPosition = ordinalPosition;
  }

  public byte[] getColumnDefault() {
    return columnDefault;
  }

  public void setColumnDefault(byte[] columnDefault) {
    this.columnDefault = columnDefault;
  }

  public boolean isNullable() {
    return isNullable;
  }

  public void setNullable(boolean nullable) {
    isNullable = nullable;
  }

  public String getDataType() {
    return dataType;
  }

  public void setDataType(String dataType) {
    this.dataType = dataType;
  }

  public Integer getCharacterMaximumLength() {
    return characterMaximumLength;
  }

  public void setCharacterMaximumLength(Integer characterMaximumLength) {
    this.characterMaximumLength = characterMaximumLength;
  }

  public Integer getCharacterOctetLength() {
    return characterOctetLength;
  }

  public void setCharacterOctetLength(Integer characterOctetLength) {
    this.characterOctetLength = characterOctetLength;
  }

  public Integer getNumericPrecision() {
    return numericPrecision;
  }

  public void setNumericPrecision(Integer numericPrecision) {
    this.numericPrecision = numericPrecision;
  }

  public Integer getNumericScale() {
    return numericScale;
  }

  public void setNumericScale(Integer numericScale) {
    this.numericScale = numericScale;
  }

  public String getCharacterSetName() {
    return characterSetName;
  }

  public void setCharacterSetName(String characterSetName) {
    this.characterSetName = characterSetName;
  }

  public String getCollationName() {
    return collationName;
  }

  public void setCollationName(String collationName) {
    this.collationName = collationName;
  }

  public String getColumnType() {
    return columnType;
  }

  public void setColumnType(String columnType) {
    this.columnType = columnType;
  }

  public String getColumnKey() {
    return columnKey;
  }

  public void setColumnKey(String columnKey) {
    this.columnKey = columnKey;
  }

  public String getExtra() {
    return extra;
  }

  public void setExtra(String extra) {
    this.extra = extra;
  }

  public String getPrivateges() {
    return privateges;
  }

  public void setPrivateges(String privateges) {
    this.privateges = privateges;
  }

  public String getColumnComment() {
    return columnComment;
  }

  public void setColumnComment(String columnComment) {
    this.columnComment = columnComment;
  }
}
