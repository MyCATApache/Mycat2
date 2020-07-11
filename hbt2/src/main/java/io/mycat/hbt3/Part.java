package io.mycat.hbt3;

import org.apache.calcite.rel.RelNode;

import java.util.List;
import java.util.Map;

public interface Part {
    String getName();

    List<Object> collectParams(Map<String, Object> context);

    String getSql(RelNode node);
}