package io.mycat.beans.mysql.infomationSchema;

/**
 * @author jamie12221
 * @date 2019-05-16 16:47
 **/
public class Schemata {

  String catalog;
  String schemaName;
  String defaultCharacterSetName;
  String defauoltCollationName;
  String sqlPath;

  public String getCatalog() {
    return catalog;
  }

  public void setCatalog(String catalog) {
    this.catalog = catalog;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  public String getDefaultCharacterSetName() {
    return defaultCharacterSetName;
  }

  public void setDefaultCharacterSetName(String defaultCharacterSetName) {
    this.defaultCharacterSetName = defaultCharacterSetName;
  }

  public String getDefauoltCollationName() {
    return defauoltCollationName;
  }

  public void setDefauoltCollationName(String defauoltCollationName) {
    this.defauoltCollationName = defauoltCollationName;
  }

  public String getSqlPath() {
    return sqlPath;
  }

  public void setSqlPath(String sqlPath) {
    this.sqlPath = sqlPath;
  }
}
