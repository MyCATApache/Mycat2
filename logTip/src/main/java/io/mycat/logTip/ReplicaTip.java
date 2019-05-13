package io.mycat.logTip;

/**
 * @author jamie12221
 * @date 2019-05-13 12:48
 **/
public enum ReplicaTip {
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

  public String getMessage(Object... args) {
    return String.format(this.getMessage(), args);
  }
}
