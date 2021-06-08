package io.mycat.calcite.logical;

import io.mycat.Partition;
import io.mycat.calcite.rewriter.Distribution;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public interface MycatViewDataNodeMapping extends Function<List<Object>, Stream<Map<String, Partition>>>, Serializable {

    boolean containsOrder();

    Distribution.Type getType();

    public Distribution distribution();

    String toJson();
}
