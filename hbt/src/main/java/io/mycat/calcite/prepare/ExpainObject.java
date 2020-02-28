package io.mycat.calcite.prepare;

import lombok.Getter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


@Getter
public class ExpainObject {
    final String sql;
    final String hbt;
    final String rel;

    public ExpainObject(String sql, String hbt, String rel) {
        this.sql = sql;
        this.hbt = hbt;
        this.rel = rel;
    }
   public static List<String> explain(String sql, String hbt, String rel){
       return new ExpainObject(sql, hbt, rel).explain();
    }

    List<String> explain(){
        ArrayList<String> list = new ArrayList<>();
        if (sql!=null) {
            list.add("sql:");
            list.addAll(Arrays.asList(sql.split("\n")));
        }
        if (hbt!=null) {
            list.add("hbt:");
            list.addAll(Arrays.asList(hbt.split("\n")));
        }
        if (rel!=null) {
            list.add("rel:");
            list.addAll(Arrays.asList(rel.split("\n")));
        }
        return list;
    }
}