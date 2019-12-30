package cn.lightfish.wu.ast.modify;

import cn.lightfish.describer.ParseNode;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class Assign {
    String identifier;
    ParseNode expr;
}