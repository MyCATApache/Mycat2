package io.mycat;

import lombok.Builder;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@ToString
@Builder
public class ExplainDetail {
    ExecuteType executeType;
    Map<String, List<String>> targets;
    String balance;
    final boolean forceProxy;
    final boolean needStartTransaction;
    boolean globalTableUpdate = false;

    public List<String> toExplain() {
        ArrayList<String> list = new ArrayList<>();
        list.add("executeType = " + executeType);
        for (Map.Entry<String, List<String>> stringListEntry : targets.entrySet()) {
            for (String s : stringListEntry.getValue()) {
                list.add("target: " + stringListEntry.getKey() + " sql:" + s);
            }
        }
        list.add("balance = " + balance);
        list.add("globalTableUpdate = " + globalTableUpdate);
        list.add("needStartTransaction = " + needStartTransaction);
        list.add("forceProxy = " + forceProxy);
        return list;
    }
}
