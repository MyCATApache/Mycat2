package io.mycat.hbt;

import io.mycat.api.collector.RowBaseIterator;

public interface HBTRunner {
   RowBaseIterator run(String text);
}