package io.mycat.plug.sequence;

public interface SequenceCallback {

  void onSequence(long value);

  void onException(Exception e, Object sender, Object attr);
}