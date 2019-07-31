package io.mycat.compute.range;

public interface Range extends Comparable<Range> {

  static Range of(long value) {
    return new Single(value);
  }

  static Range of(long start, long end) {
    return new SeqRange(start, end);
  }

  long getEnd();
}