package io.mycat.hbt.ast.modify;

import io.mycat.describer.ParseNode;
import io.mycat.hbt.ast.base.Node;

import java.util.List;

public class MergeStatement {
    List<ParseNode> sources;
    ParseNode booleanExpression;
    List<Assign> assigns;
    List<Node> values;
}