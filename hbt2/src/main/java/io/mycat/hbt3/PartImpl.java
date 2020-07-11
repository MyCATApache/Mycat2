package io.mycat.hbt3;

import lombok.AllArgsConstructor;
import org.apache.calcite.rel.RelNode;

import java.util.List;
import java.util.Map;

@AllArgsConstructor
public class PartImpl implements Part {
    String name;
    @Override
    public String getName() {
        return name;
    }

    @Override
    public List<Object> collectParams(Map<String, Object> context) {
        return null;
    }

    @Override
    public String getSql(RelNode relNode) {
        return null;
    }
}