package io.mycat;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;



@Getter
public class ExplainDetail {
    private final ExecuteType executeType;
    private final String target;
    private final String sql;
    private final String balance;

    public ExplainDetail(ExecuteType executeType,
                         String target,
                         String sql,
                         String balance) {
        this.executeType = executeType;
        this.target = target;
        this.sql = sql;
        this.balance = balance;
    }

    public static ExplainDetail create(ExecuteType executeType,
                                       String target,
                                       String sql,
                                       String balance
           ){
        return new ExplainDetail(executeType,
                target,sql,
                balance);
    }

    public List<String> toExplain() {
        ArrayList<String> list = new ArrayList<>();
        list.add("executeType:" + executeType);
        list.add("target: " + target);
        list.add( "sql:" + sql);
        list.add("balance:" + balance);
        return list;
    }

    @Override
    public String toString() {
        return String.join("\n", toExplain());
    }
}
