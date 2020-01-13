package io.mycat.router.migrate;

import io.mycat.router.function.NodeIndexRange;
import lombok.Getter;

import java.util.List;
import java.util.Objects;


/**
 * 迁移任务
 *
 * @author chenjunwen
 */

@Getter
public class MigrateTask {

    private final String from;
    private final String to;
    private final List<NodeIndexRange> slots;

    public MigrateTask(String from, String to, List<NodeIndexRange> slots) {
        this.from = from;
        this.to = to;
        this.slots = Objects.requireNonNull(slots);
    }

    public long getSize() {
        return this.slots.stream().map(i -> i.getSize()).count();
    }

    @Override
    public String toString() {
        return "MigrateTask{" +
                "from='" + from + '\'' +"\n"+
                ", to='" + to + '\'' +"\n"+
                ", slots=" + slots  +"\n"+
                '}';
    }
}
