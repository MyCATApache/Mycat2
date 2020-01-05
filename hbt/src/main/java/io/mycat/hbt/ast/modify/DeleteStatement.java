package io.mycat.hbt.ast.modify;

import io.mycat.describer.ParseNode;
import lombok.AllArgsConstructor;

import java.util.List;

@AllArgsConstructor
public class DeleteStatement {
    List<ParseNode> sources;
}