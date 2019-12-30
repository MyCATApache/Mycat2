package cn.lightfish.wu;

import cn.lightfish.wu.ast.base.Node;
import cn.lightfish.wu.ast.modify.RowModifer;

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