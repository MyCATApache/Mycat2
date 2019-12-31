package io.mycat.wu.ast.base;

import io.mycat.wu.Op;
import lombok.Data;

import java.util.List;
import java.util.stream.Collectors;

@Data
public class Fun extends Expr {
    final String functionName;
    final String alias;

    public Fun(String functionName, String alias, List<Expr> nodes) {
        super(Op.FUN, nodes);
        this.functionName = functionName;
        this.alias = alias;
    }

    @Override
    public String toString() {
        return functionName + "(" +
                nodes.stream().map(i -> i.toString()).collect(Collectors.joining(",")) +
                ')';
    }
}