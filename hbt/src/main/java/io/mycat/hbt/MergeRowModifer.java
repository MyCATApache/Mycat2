package io.mycat.hbt;

import io.mycat.hbt.ast.base.Node;
import io.mycat.hbt.ast.modify.RowModifer;

import java.util.List;

public class MergeRowModifer implements RowModifer {
    private final Node matcher;
    private final List<RowModifer> rowModifer;
    private final List<Node> values;

    public MergeRowModifer(Node matcher, List<RowModifer> rowModifer, List<Node> values) {

        this.matcher = matcher;
        this.rowModifer = rowModifer;
        this.values = values;
    }
}