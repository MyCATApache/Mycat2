/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.plug.sequence.sequencefile;

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