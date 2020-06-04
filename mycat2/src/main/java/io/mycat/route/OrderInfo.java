package io.mycat.route;

import lombok.Getter;

@Getter
public class OrderInfo {
    final String[] columns;
    final boolean[] orders;

    public OrderInfo(String[] columns, boolean[] orders) {
        this.columns = columns;
        this.orders = orders;
    }

    public static OrderInfo create(String[] columns, boolean[] orders) {
        return new OrderInfo(columns, orders);
    }

}