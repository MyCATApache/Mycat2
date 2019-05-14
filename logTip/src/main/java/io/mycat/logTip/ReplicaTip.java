package io.mycat.logTip;

/**
 * @author jamie12221
 * @date 2019-05-13 12:48
 * 本类管理所有Replica相关的日志或者异常提示
 **/
public enum ReplicaTip implements LogTip {
  ERROR_EXECUTION_THREAD("Replica must running in MycatReactorThread"),
  NO_AVAILABLE_DATA_SOURCE("No available Replica %s"),
  INIT_REPLICA("init Replica %s"),
  ILLEGAL_REPLICA_INDEX("index of % in replica-index.yml"),
  ;
  String message;

  ReplicaTip(String message) {
    this.message = message;
  }

  public String getMessage() {
    return message;
  }
}
