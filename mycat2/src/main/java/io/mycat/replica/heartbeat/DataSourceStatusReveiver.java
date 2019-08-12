package io.mycat.replica.heartbeat;

public interface DataSourceStatusReveiver {

  void reveive(DatasourceStatus status);
}