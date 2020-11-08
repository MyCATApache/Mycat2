package io.mycat.hbt4;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
public class GroupKey {
    String parameterizedSql;
    String target;

    private GroupKey(String parameterizedSql, String target) {
        this.parameterizedSql = parameterizedSql;
        this.target = target;
    }

    private GroupKey() {
    }

    public static GroupKey of(String parameterizedSql, String target){
        GroupKey groupKey = new GroupKey(parameterizedSql,target);
        return groupKey;
    }
}
