package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by jamie on 2017/9/12.
 */
public class TrieContext {
    public List<String> funList = new ArrayList<>();
    public int x = 0;
    public int y = 0;
    public boolean isBacktracking=false;
    public int index=0;
    Map<String, Integer> posMap;

    // public boolean isQuesMark=false;
    public String genFun(String str) {
        return String.format("_%s_%d_%d", str.replace("\"",""), x, y);
    }


}
