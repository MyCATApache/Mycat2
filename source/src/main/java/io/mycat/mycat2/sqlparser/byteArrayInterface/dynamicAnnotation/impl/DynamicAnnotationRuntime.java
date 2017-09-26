package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.impl;

import io.mycat.mycat2.sqlparser.BufferSQLContext;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

/**
 * Created by jamie on 2017/9/5.
 */
public class DynamicAnnotationRuntime {
    Map<String, Set<String>> map ;//描述条件(包含?)之间的包含关系
    Map<Integer, String> int2str ;//id与条件
    Map<String, Integer> str2Int ;//条件与id
    Map<String, Integer> backtrackingTable;
    DynamicAnnotationMatch match;
    String codePath;
    String matchName;
    boolean isDebug=true;

    public DynamicAnnotationRuntime(Map<String, Set<String>> map, Map<Integer, String> int2str, Map<String, Integer> str2Int) {
        this.map = map;
        this.int2str = int2str;
        this.str2Int = str2Int;
    }

    public DynamicAnnotationRuntime() {

    }

    public Map<String, Set<String>> getMap() {
        return map;
    }
    public void testCallBackInfo(BufferSQLContext c)throws Exception {
        if (isDebug) {
            System.out.println(Arrays.toString(match.getCompleteTags()));
            int2str.entrySet().stream().forEach(e -> System.out.println(e.getKey() + ":" + e.getValue()));
            if (!match.isComplete()) {
                throw new Exception("没有匹配完全");
            }
        }
    }

    public void setMap(Map<String, Set<String>> map) {
        this.map = map;
    }

    public Map<Integer, String> getInt2str() {
        return int2str;
    }

    public void setInt2str(Map<Integer, String> int2str) {
        this.int2str = int2str;
    }

    public Map<String, Integer> getStr2Int() {
        return str2Int;
    }

    public void setStr2Int(Map<String, Integer> str2Int) {
        this.str2Int = str2Int;
    }

    public DynamicAnnotationMatch getMatch() {
        return match;
    }

    public void setMatch(DynamicAnnotationMatch match) {
        this.match = match;
    }

    public String getCodePath() {
        return codePath;
    }

    public void setCodePath(String codePath) {
        this.codePath = codePath;
    }

    public String getMatchName() {
        return matchName;
    }

    public void setMatchName(String matchName) {
        this.matchName = matchName;
    }

    public boolean isDebug() {
        return isDebug;
    }

    public void setDebug(boolean debug) {
        isDebug = debug;
    }

    public Map<String, Integer> getBacktrackingTable() {
        return backtrackingTable;
    }

    public void setBacktrackingTable(Map<String, Integer> backtrackingTable) {
        this.backtrackingTable = backtrackingTable;
    }

    @Override
    public String toString() {
        return "DynamicAnnotationRuntime{" +
                "map=" + map +
                ", int2str=" + int2str +
                ", str2Int=" + str2Int +
                ", backtrackingTable=" + backtrackingTable +
                ", match=" + match +
                ", codePath='" + codePath + '\'' +
                ", matchName='" + matchName + '\'' +
                ", isDebug=" + isDebug +
                '}';
    }
}
