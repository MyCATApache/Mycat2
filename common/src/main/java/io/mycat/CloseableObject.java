package io.mycat;

public interface CloseableObject extends Cloneable {

  void onExceptionClose();

  void close();
}