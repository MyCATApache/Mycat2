package io.mycat.describer;

import lombok.Getter;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Getter
public class ParenthesesExpr implements ParseNode {

    private final List<ParseNode> exprs;
    public ParenthesesExpr(ParseNode... exprs){
    this.exprs = Arrays.asList(exprs);
    }
    public ParenthesesExpr(List<ParseNode> exprs) {
        this.exprs = exprs;
    }
    public ParenthesesExpr(ParseNode exprs) {
        this.exprs = Collections.singletonList(exprs);
    }

    @Override
    public void accept(ParseNodeVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    @Override
    public ParenthesesExpr copy() {
        return new ParenthesesExpr((ParseNode) exprs.stream().map(i -> i.copy()).collect(Collectors.toList()));
    }

    @Override
    public String toString() {
        return MessageFormat.format( "({0})",  exprs.stream().map(i->Objects.toString(i)).collect(Collectors.joining(",")));
    }
}