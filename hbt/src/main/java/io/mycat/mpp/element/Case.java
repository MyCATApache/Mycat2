//package io.mycat.mpp.element;
//
//import io.mycat.sqlabs.SqlAbsExpr;
//import lombok.AllArgsConstructor;
//import lombok.Data;
//
//import java.util.ArrayList;
//import java.util.List;
//
//
//public class Case extends SqlAbsExpr {
//
//    List<Item> items = new ArrayList<>();
//    SqlAbsExpr value;
//    SqlAbsExpr _else;
//    public void addItem(SqlAbsExpr condition, SqlAbsExpr value) {
//        items.add(new Item(condition,value));
//    }
//
//    public void setValue(SqlAbsExpr c) {
//        value = c;
//    }
//
//    public void setElse(SqlAbsExpr c) {
//        _else = c;
//    }
//
//    @Data
//    @AllArgsConstructor
//    public static class Item{
//        SqlAbsExpr condition;
//        SqlAbsExpr value;
//    }
//
//    public List<Item> getItems() {
//        return items;
//    }
//
//    public void setItems(List<Item> items) {
//        this.items = items;
//    }
//
//    public SqlAbsExpr getValue() {
//        return value;
//    }
//
//    public SqlAbsExpr getElse() {
//        return _else;
//    }
//
//
//}