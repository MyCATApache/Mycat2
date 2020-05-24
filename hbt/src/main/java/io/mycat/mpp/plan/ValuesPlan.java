package io.mycat.mpp.plan;

import io.mycat.mpp.DataContext;
import lombok.Builder;

import java.util.List;

@Builder
public class ValuesPlan extends QueryPlan {
    final Type type;
    final List<Object[]> values;

    public static ValuesPlan create(Type type,List<Object[]> values){
        return new ValuesPlan(type,values);
    }


    public ValuesPlan(Type type,List<Object[]> values) {
        this.values = values;
        this.type = type;
    }

    @Override
    public Type getColumns() {
        return type;
    }

    @Override
    public Scanner scan(DataContext dataContext, long flags) {
        return Scanner.of(values.stream().map(i -> DataAccessor.of(i))
                .iterator());
    }
}