package io.mycat.replica;

public interface PhysicsInstance {

  int getIndex();

  String getName();

  boolean isMaster();

  boolean asSelectRead();

  boolean isAlive();

  int getSessionCounter();

  int getWeight();
}