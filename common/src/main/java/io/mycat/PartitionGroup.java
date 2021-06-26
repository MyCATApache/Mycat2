package io.mycat;

import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Map;

@Getter
@EqualsAndHashCode
public class PartitionGroup {
    final String targetName;
    final  Map<String, Partition> map;

    public PartitionGroup(String targetName, Map<String, Partition> map) {
        this.targetName = targetName;
        this.map = map;
    }

    public Partition get(String uniqueName) {
        return map.get(uniqueName);
    }
}
