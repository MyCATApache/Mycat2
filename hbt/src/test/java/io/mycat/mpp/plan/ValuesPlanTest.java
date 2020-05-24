package io.mycat.mpp.plan;

import com.sun.org.apache.xpath.internal.operations.Or;
import io.mycat.beans.mysql.MySQLType;
import io.mycat.mpp.DataContext;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ValuesPlanTest {
   final DataContext dataContext = DataContext.DEFAULT;

    @Test
    public void test(){

        ValuesPlan valuesPlan = ValuesPlan.create(
                Type.of(
                        Column.of("id", Integer.class),
                        Column.of("name", String.class)
                        ),
                values(new Object[]{1,"1"}, new Object[]{2,"2"})
                );
        Type columns = valuesPlan.getColumns();
        Scanner scan = valuesPlan.scan(dataContext, 0);
        String collect = scan.stream().map(i -> i.toString())
                .collect(Collectors.joining());

        OrderPlan orderPlan = OrderPlan.create(valuesPlan, new int[]{1}, new boolean[]{false});
        String collect1 = orderPlan.scan(dataContext, 0).stream().map(i -> i.toString()).collect(Collectors.joining());

        LimitPlan limitPlan =  LimitPlan.create(orderPlan,0,1);

        Scanner scan1 = limitPlan.scan(dataContext, 0);
        String collect2 = scan1.stream().map(i -> i.toString()).collect(Collectors.joining());

        AggregationPlan aggregationPlan = AggregationPlan.create(limitPlan, new String[]{"count","avg"}, Type.of(
                Column.of("count()", Long.class),
                Column.of("avg(id)", Double.class)
                ),
                Collections.singletonList( Collections.singletonList(1)), new int[]{});

        String collect3 = aggregationPlan.scan(dataContext, 0).stream().map(i -> i.toString()).collect(Collectors.joining());
    }

    private List<Object[]> values(Object[]... objects) {
        return Arrays.asList(objects);
    }

}