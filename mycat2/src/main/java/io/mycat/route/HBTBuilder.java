package io.mycat.route;

import io.mycat.hbt.ast.HBTOp;
import io.mycat.hbt.ast.base.OrderItem;
import io.mycat.hbt.ast.base.Schema;
import io.mycat.hbt.ast.query.FromSqlSchema;
import io.mycat.hbt.ast.query.SetOpSchema;
import org.checkerframework.checker.units.qual.A;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class HBTBuilder {
    LinkedList<Schema> stack;

    public static HBTBuilder create() {
        return new HBTBuilder();
    }

    public HBTBuilder from(String targetName, String sql) {
        stack.push(new FromSqlSchema(Collections.emptyList(), targetName, sql));
        return this;
    }

    public HBTBuilder distinct() {
        return null;
    }

    public HBTBuilder unionMore(boolean all) {
        ArrayList<Schema> arrayList = new ArrayList<>();
        while (stack.isEmpty()){
            arrayList.add(stack.pop());
        }
//        stack.push(new SetOpSchema(all?HBTOp.UNION_ALL,));
        return null;
    }

    public HBTBuilder order(List<OrderItem> concertOrder) {
        return null;
    }

    public HBTBuilder limit(List<OrderItem> concertOrder) {
        return null;
    }

    public Schema build() {
        return stack.pop();
    }
}