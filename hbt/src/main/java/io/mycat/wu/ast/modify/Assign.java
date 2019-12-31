package io.mycat.wu.ast.modify;

import io.mycat.describer.ParseNode;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class Assign {
    String identifier;
    ParseNode expr;
}