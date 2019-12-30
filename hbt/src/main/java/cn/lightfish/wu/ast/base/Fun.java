package cn.lightfish.wu.ast.base;

import cn.lightfish.wu.Op;
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