package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation;

import io.mycat.mycat2.sqlparser.BufferSQLContext;

import java.util.*;

/**
 * Created by jamie on 2017/9/5.
 */
public class DynamicAnnotationRuntime {
    Map<String, Set<String>> map ;//描述条件(包含?)之间的包含关系
    Map<Integer, String> int2str ;//id与条件
    Map<String, Integer> str2Int ;//条件与id
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
    public void printCallBackInfo(BufferSQLContext c) {
        if (isDebug){
            int[] r= c.getDynamicAnnotationResultList();
            int size=c.getDynamicAnnotationResultIndex();
            for (int i = 0; i < size; i++) {
                Set<String> strings=  map.get(int2str.get(r[i]));
                for (String it:strings){
                    System.out.println("配对:"+it);
                }
            }
            c.clearDynamicAnnotationResultList();
        }
    }
    public void testCallBackInfo(BufferSQLContext c)throws Exception {
        if (isDebug){
            int[] r= c.getDynamicAnnotationResultList();
            int size=c.getDynamicAnnotationResultIndex();
            HashSet<String> d=new HashSet<>();
            for (int i = 0; i < size; i++) {
                String it = int2str.get(r[i]);
                d.add(it);
            }
              Set<String> res=  str2Int.keySet();
            d.stream().forEach((s)->System.out.println(s));
              res.removeAll(d);
                if (res.size()!=0){
                    throw new Exception("没有匹配完全");
                }
            }
            c.clearDynamicAnnotationResultList();

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
}
