package io.mycat.beans.mycat;

public interface MycatDataSource {

  int getIndex();

  boolean isAlive();

  boolean asSelectRead();
}