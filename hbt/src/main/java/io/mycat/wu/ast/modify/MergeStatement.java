package io.mycat.wu.ast.modify;

import io.mycat.describer.ParseNode;
import io.mycat.wu.ast.base.Node;

import java.util.List;

public class MergeStatement {
    List<ParseNode> sources;
    ParseNode booleanExpression;
    List<Assign> assigns;
    List<Node> values;
}