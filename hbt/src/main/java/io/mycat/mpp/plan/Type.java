package io.mycat.mpp.plan;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.util.Comparator;

@Builder
@AllArgsConstructor
@Getter
@ToString
public class Type {
    final Column[] columns;
    public Comparator<DataAccessor> getComparator(int index) {
        return columns[index].createComparator(index);
    }

    public static  Type of(Column... columns){
        return new Type(columns);
    }
}