package io.mycat.plug.sequence.SequenceTime;

import io.mycat.plug.sequence.SequenceCallback;
import io.mycat.plug.sequence.SequenceHandler;

/**
 * @todo enhance
 */
public class SequenceTimeHandler implements SequenceHandler {

  long lastTime;

  @Override
  public void nextId(SequenceCallback callback) {
    long l = System.currentTimeMillis();
    synchronized (this) {
      if (lastTime < l) {
        lastTime = l;
      } else {
        l = lastTime += 1;
      }
    }
    callback.onSequence(l);
  }
}