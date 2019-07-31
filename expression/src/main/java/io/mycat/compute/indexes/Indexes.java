package io.mycat.compute.indexes;

import java.util.Collections;
import java.util.List;

public class Indexes {

  final Index globalIndex;
  List<Index> subIndexes = Collections.emptyList();

  public Indexes(Index globalIndex) {
    this.globalIndex = globalIndex;
  }

  public Indexes(Index globalIndex, List<Index> subIndexes) {
    this.globalIndex = globalIndex;
    this.subIndexes = subIndexes;
  }

}