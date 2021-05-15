package io.mycat;

import lombok.Data;

import java.util.List;

@Data
public class HintRoute {
    Boolean master;
    Boolean slave;
    List<String> indexes;
    List<String> logicalTables;
    List<String> physicalTables;
    List<String> targets;
}