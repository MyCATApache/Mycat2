package io.mycat.replica;

public interface PhysicsInstance {

  String getName();

  boolean isMaster();

  boolean asSelectRead();

  boolean isAlive();

  int getSessionCounter();

  int getWeight();
}