package io.mycat.calcite.executor;

import io.mycat.DataNode;
import lombok.*;

import java.util.Objects;

@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@ToString
public class GroupKey {
    String parameterizedSql;
    String target;
    DataNode dataNode;

    public static GroupKey of(String parameterizedSql, String target, DataNode dataNode){
        GroupKey groupKey = new GroupKey(parameterizedSql,target,dataNode);
        return groupKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GroupKey that = (GroupKey) o;
        return Objects.equals(that.target,this.target)
                && Objects.equals(that.parameterizedSql,this.parameterizedSql);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.parameterizedSql,this.target);
    }
}
