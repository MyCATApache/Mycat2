package io.mycat.router.sequence;

public interface SequenceHandler {

  void nextId(SequenceCallback callback);
}