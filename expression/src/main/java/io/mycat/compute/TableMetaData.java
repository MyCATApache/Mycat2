package io.mycat.compute;

import io.mycat.compute.range.Range;
import java.util.List;
import java.util.TreeMap;

public class TableMetaData {

  private final TreeMap<Integer, List<Range>> rangeMap = new TreeMap<>();

}