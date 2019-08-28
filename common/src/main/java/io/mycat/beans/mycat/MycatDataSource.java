package io.mycat.beans.mycat;

import io.mycat.replica.PhysicsInstance;

public interface MycatDataSource {

  int getIndex();

  String getName();

  MycatReplica getReplica();

  PhysicsInstance instance();
}