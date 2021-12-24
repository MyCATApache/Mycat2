package io.mycat;

import com.alibaba.druid.sql.ast.SQLStatement;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


@Getter
public class ExplainDetail {
    private final ExecuteType executeType;
    private final List<String> targets;
    private final String sql;
    private final String balance;
    private final List<Object> params;

    public ExplainDetail(ExecuteType executeType,
                         List<String> targets,
                         String sql,
                         String balance,
                         List<Object> params) {
        this.executeType = executeType;
        this.targets = targets;
        this.sql = sql;
        this.balance = balance;
        this.params = params;
    }

    public static ExplainDetail create(ExecuteType executeType,
                                       List<String> targets,
                                       String sql,
                                       String balance,
                                       List<Object> params
    ) {
        return new ExplainDetail(executeType,
                targets, sql,
                balance, params);
    }

    public List<String> toExplain() {
        ArrayList<String> list = new ArrayList<>();
        list.add("executeType:" + executeType);
        list.add("targets: " + targets);
        list.add("sql:" + sql);
        list.add("balance:" + balance);
        return list;
    }

    @Override
    public String toString() {
        return String.join("\n", toExplain());
    }
}
