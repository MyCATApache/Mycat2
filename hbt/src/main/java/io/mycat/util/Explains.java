package io.mycat.util;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


@Getter
public class Explains {
    final String sql;
    final String prepareCompute;
    final String hbt;
    final String rel;

    @AllArgsConstructor
    @ToString
    public static class PrepareCompute{
        String targetName;
        String sql;
        List<Object> params;

        @Override
        public String toString() {
            return
                    "targetName='" + targetName +"\'\n" +
                    ", sql='" + sql + "\'\n" +
                    ", params=" + params +"\n";
        }
    }

    public Explains(String sql,String prepareCompute, String hbt, String rel) {
        this.sql = sql;
        this.prepareCompute = prepareCompute;
        this.hbt = hbt;
        this.rel = rel;
    }
   public static List<String> explain(String sql,String prepareCompute, String hbt, String rel){
       return new Explains(sql,prepareCompute, hbt, rel).explain();
    }

    List<String> explain(){
        ArrayList<String> list = new ArrayList<>();
        if (!StringUtil.isEmpty(sql)) {
            list.add("sql:");
            list.addAll(Arrays.asList(sql.split("\n")));
        }
        if (!StringUtil.isEmpty(prepareCompute)) {
            list.add("");
            list.add("prepareCompute:");
            list.addAll(Arrays.asList(prepareCompute.split("\n")));
        }
        if (!StringUtil.isEmpty(hbt)) {
            list.add("");
            list.add("hbt:");
            list.addAll(Arrays.asList(hbt.split("\n")));
        }
        if (!StringUtil.isEmpty(rel)) {
            list.add("");
            list.add("rel:");
            list.addAll(Arrays.asList(rel.split("\n")));
        }
        return list;
    }
}