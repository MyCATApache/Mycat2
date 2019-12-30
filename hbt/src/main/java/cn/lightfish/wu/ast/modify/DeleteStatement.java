package cn.lightfish.wu.ast.modify;

import cn.lightfish.describer.ParseNode;
import lombok.AllArgsConstructor;

import java.util.List;

@AllArgsConstructor
public class DeleteStatement {
    List<ParseNode> sources;
}