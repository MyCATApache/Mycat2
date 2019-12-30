package cn.lightfish.rsqlBuilder;

import cn.lightfish.describer.ParseNode;

public interface DotAble extends ParseNode {
    <T> T dot(String o);
}