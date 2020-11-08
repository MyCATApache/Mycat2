package io.mycat.router.migrate;

import io.mycat.router.NodeIndexRange;
import io.mycat.router.mycat1xfunction.ConsistentHashPreSlot;
import lombok.Getter;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;


@Getter
public class ConsistentHashBalanceExpandResult {
    SortedMap<String, List<MigrateTask>> migrateTaskMap;
    ConsistentHashPreSlot consistentHash;

    public ConsistentHashBalanceExpandResult(SortedMap<String, List<MigrateTask>> migrateTaskMap, ConsistentHashPreSlot consistentHash) {
        this.migrateTaskMap = migrateTaskMap;
        this.consistentHash = consistentHash;
    }

    public void run(Runner r) {
        for (Map.Entry<String, List<MigrateTask>> entry : migrateTaskMap.entrySet()) {
            String dataNode = entry.getKey();//目标数据源
            List<MigrateTask> tasks = entry.getValue();//来源数据源
            ///////////////////////////////////////////////////
            for (MigrateTask task : tasks) {
                String from = task.getFrom();;
                String to = task.getTo();//目标数据源

                for (NodeIndexRange slot : task.getSlots()) {
                    long valueStart = slot.getValueStart();
                    long valueEnd = slot.getValueEnd();
                    r.run(from,to,valueStart,valueEnd);
                }
            }
        }
    }

    interface Runner{
       public void run(String from, String to, long valueStart, long valueEnd);
    }

}
