package io.mycat;

public interface CloseableObject {

  void onExceptionClose();

  void close();
}