package io.mycat.plug.sequence;

/**
 * @todo enhance
 */
public class SequenceTimeHandler implements SequenceHandler {

  @Override
  public void nextId(SequenceCallback callback) {
    callback.onSequence(System.currentTimeMillis());
  }
}