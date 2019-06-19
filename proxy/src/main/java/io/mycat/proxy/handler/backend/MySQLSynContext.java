package io.mycat.proxy.handler.backend;

import io.mycat.beans.mycat.MySQLDataNode;
import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.session.MycatSession;
import java.util.Objects;

public class MySQLSynContext {

  String dataNodeName;
  MySQLDataNode dataNode;
  MySQLIsolation isolation;
  MySQLAutoCommit autoCommit;
  String charset;
  String characterSetResult;
  ;

  public MySQLSynContext(MycatSession session) {
    this.dataNodeName = session.getDataNode();
    this.dataNode = getDataNode();
    this.isolation = session.getIsolation();
    this.autoCommit = session.getAutoCommit();
    this.charset = session.getCharsetName();
    this.characterSetResult = session.getCharacterSetResults();
  }

  public MySQLSynContext(MySQLDataNode dataNode, MySQLIsolation isolation,
      MySQLAutoCommit autoCommit, String charset, String characterSetResult) {
    this.dataNode = dataNode;
    this.isolation = isolation;
    this.autoCommit = autoCommit;
    this.charset = charset;
    this.characterSetResult = characterSetResult;
  }

  public MySQLSynContext(String dataNodeName, MySQLIsolation isolation,
      MySQLAutoCommit autoCommit, String charset, String characterSetResult) {
    this.dataNodeName = dataNodeName;
    this.isolation = isolation;
    this.autoCommit = autoCommit;
    this.charset = charset;
    this.characterSetResult = characterSetResult;
  }

  /**
   * Getter for property 'dataNodeName'.
   *
   * @return Value for property 'dataNodeName'.
   */
  public String getDataNodeName() {
    if (dataNodeName==null){
      Objects.requireNonNull(this.dataNode);
      dataNodeName = this.dataNode.getName();
    }
    return dataNodeName;
  }

  /**
   * Setter for property 'dataNodeName'.
   *
   * @param dataNodeName Value to set for property 'dataNodeName'.
   */
  public void setDataNodeName(String dataNodeName) {
    this.dataNodeName = dataNodeName;
  }

  /**
   * Getter for property 'dataNode'.
   *
   * @return Value for property 'dataNode'.
   */
  public MySQLDataNode getDataNode() {
    if (dataNode == null) {
      Objects.requireNonNull(dataNodeName);
      this.dataNode = ProxyRuntime.INSTANCE.getDataNodeByName(dataNodeName);
    }
    return dataNode;
  }

  /**
   * Setter for property 'dataNode'.
   *
   * @param dataNode Value to set for property 'dataNode'.
   */
  public void setDataNode(MySQLDataNode dataNode) {
    this.dataNode = dataNode;
  }

  /**
   * Getter for property 'isolation'.
   *
   * @return Value for property 'isolation'.
   */
  public MySQLIsolation getIsolation() {
    return isolation;
  }

  /**
   * Setter for property 'isolation'.
   *
   * @param isolation Value to set for property 'isolation'.
   */
  public void setIsolation(MySQLIsolation isolation) {
    this.isolation = isolation;
  }

  /**
   * Getter for property 'autoCommit'.
   *
   * @return Value for property 'autoCommit'.
   */
  public MySQLAutoCommit getAutoCommit() {
    return autoCommit;
  }

  /**
   * Setter for property 'autoCommit'.
   *
   * @param autoCommit Value to set for property 'autoCommit'.
   */
  public void setAutoCommit(MySQLAutoCommit autoCommit) {
    this.autoCommit = autoCommit;
  }

  /**
   * Getter for property 'charset'.
   *
   * @return Value for property 'charset'.
   */
  public String getCharset() {
    return charset;
  }

  /**
   * Setter for property 'charset'.
   *
   * @param charset Value to set for property 'charset'.
   */
  public void setCharset(String charset) {
    this.charset = charset;
  }

  /**
   * Getter for property 'characterSetResult'.
   *
   * @return Value for property 'characterSetResult'.
   */
  public String getCharacterSetResult() {
    return characterSetResult;
  }

  /**
   * Setter for property 'characterSetResult'.
   *
   * @param characterSetResult Value to set for property 'characterSetResult'.
   */
  public void setCharacterSetResult(String characterSetResult) {
    this.characterSetResult = characterSetResult;
  }
}