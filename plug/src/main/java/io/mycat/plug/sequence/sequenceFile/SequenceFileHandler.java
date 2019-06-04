package io.mycat.plug.sequence.sequenceFile;

import io.mycat.plug.sequence.SequenceCallback;
import io.mycat.plug.sequence.SequenceHandler;
import java.io.IOException;

public class SequenceFileHandler implements SequenceHandler {

  final SequenceFile file;
  final int attempt = 1;

  public SequenceFileHandler(SequenceFile file) {
    this.file = file;
  }

  @Override
  public void nextId(SequenceCallback callback) {
    int counter = 0;
    for (; counter < attempt; counter++) {
      if (innerNextId(callback)) {
        return;
      } else {
        try {
          file.fetchNextPeriod();
          continue;
        } catch (Exception e) {
          callback.onException(e, this, null);
          return;
        }
      }
    }
    callback.onException(new Exception("can not get sequence"), this, null);
  }


  private boolean innerNextId(SequenceCallback callback) {
    long currentValue = file.getCurrentValue() + 1;
    long maxValue = file.getMaxValue();
    boolean valid = currentValue > maxValue;
    if (valid) {
      file.updateCurrentValue(currentValue);
      try {
        callback.onSequence(currentValue);
      } catch (Exception e) {
        callback.onException(e, null, null);
      }
      return true;
    } else {
      return false;
    }
  }

  public interface SequenceFile {

    long getCurrentValue();

    long setCurrentValue(long currentValue);

    long getMaxValue();

    long setMaxValue(long maxValue);

    long getMinValue();

    long setMinValue(long minValue);

    byte[] getHistorySequence();

    void updateCurrentValue(long currentValue);

    void fetchNextPeriod() throws IOException;


  }
}