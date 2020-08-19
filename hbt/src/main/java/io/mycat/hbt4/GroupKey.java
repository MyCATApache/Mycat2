package io.mycat.hbt4;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode
public class GroupKey {
    String parameterizedSql;
    String target;
    public static GroupKey of(String parameterizedSql,String target){
        GroupKey groupKey = new GroupKey();
        groupKey.setParameterizedSql(parameterizedSql);
        groupKey.setTarget(target);
        return groupKey;
    }
}
