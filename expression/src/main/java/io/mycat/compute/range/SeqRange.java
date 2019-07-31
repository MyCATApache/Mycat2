package io.mycat.compute.range;

public class SeqRange implements Range {

  final long start;
  final long end;
  final long size;

  public SeqRange(long start, long end) {
    this.start = start;
    this.end = end;
    this.size = end - start + 1;
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }

  public long getSize() {
    return size;
  }

  @Override
  public int compareTo(Range o) {
    return Long.compare(end, o.getEnd());
  }
}