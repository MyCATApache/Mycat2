package io.mycat.describer;

public interface ParseNode {
    void accept(ParseNodeVisitor visitor);

    <T extends ParseNode> T copy();
}