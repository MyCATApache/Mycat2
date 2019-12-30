package cn.lightfish.wu.ast.modify;

import cn.lightfish.describer.ParseNode;

import java.util.List;

public class MergeStatement {
    List<ParseNode> sources;
    ParseNode booleanExpression;
    List<Assign> assigns;
    List<cn.lightfish.wu.ast.base.Node> values;
}