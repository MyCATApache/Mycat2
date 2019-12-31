package io.mycat.rsqlBuilder;

import io.mycat.describer.ParseNode;

public interface DotAble extends ParseNode {
    <T> T dot(String o);
}