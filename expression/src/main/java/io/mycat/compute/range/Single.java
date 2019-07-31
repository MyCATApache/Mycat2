package io.mycat.compute.range;

public class Single implements Range {

  final long end;

  public Single(long end) {
    this.end = end;
  }

  @Override
  public long getEnd() {
    return this.end;
  }

  @Override
  public int compareTo(Range o) {
    return Long.compare(end, o.getEnd());
  }
}