package io.mycat.hbt;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.hbt.ast.base.Schema;

public interface HBTRunner {
   RowBaseIterator run(String text);
   RowBaseIterator run(Schema schema);
}