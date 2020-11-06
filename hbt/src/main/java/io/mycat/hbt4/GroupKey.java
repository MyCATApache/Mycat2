package io.mycat.hbt4;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
@AllArgsConstructor
public class GroupKey {
    String parameterizedSql;
    String target;
    public static GroupKey of(String parameterizedSql,String target){
        GroupKey groupKey = new GroupKey(parameterizedSql,target);
        return groupKey;
    }
}
