package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by jamie on 2017/9/14.
 */
public class ConditionUtil {
    public static String codeIsComplete(Map<Boolean, List<String>> map, DynamicAnnotationRuntime runtime) {
        Map<String, Integer> str2int = runtime.str2Int;
        List<String> andList = map.get(Boolean.TRUE);
        if (andList != null && andList.size() != 0) {
            andList = andList.stream().map((i) -> "tags[" + String.valueOf(str2int.get(i))+"]==1").collect(Collectors.toList());
            map.put(Boolean.TRUE, andList);
        }
        List<String> orList = map.get(Boolean.FALSE);
        if (orList != null && orList.size() != 0) {
            orList = orList.stream().map((i) -> "tags[" + String.valueOf(str2int.get(i))+"]==1").collect(Collectors.toList());
            map.put(Boolean.FALSE, orList);
        }
        return codeByString(map);
    }

    public static String codeByString(Map<Boolean, List<String>> map) {
        List<String> andList = map.get(Boolean.TRUE);
        String andString = "";
        String orString = "";
        if (andList != null && andList.size() != 0) {
            andString = codeAnd(andList.iterator());
        }
        List<String> orList = map.get(Boolean.FALSE);
        if (orList != null && orList.size() != 0) {
            orString = codeOr(orList.iterator());
        }
        System.out.println(andString);
        System.out.println(orString);
        return andString + orString + "\nreturn false;";
    }

    public static String codeAnd(Iterator<String> iterator) {
        if (iterator.hasNext()) {
            return genIfElse(iterator.next(), codeAnd(iterator));
        } else {
            return "\nreturn true;";
        }
    }

    public static String codeOr(Iterator<String> iterator) {
        StringBuilder stringBuilder = new StringBuilder();
        while (iterator.hasNext()) {
            stringBuilder.append(genIfElse(iterator.next(), "\nreturn true;"));
        }
        return stringBuilder.toString();
    }

    public static String genIfElse(String condition, String body) {
        return "\nif(" + condition + "){\n" + body + "\n}\n";
    }

    public final static String mappingKeyInAndOr(Map<String, String> i) {
        String res = i.get("and");
        if (res != null) {
            return "and";
        }
        res = i.get("AND");
        if (res != null) {
            return "AND";
        }
        res = i.get("or");
        if (res != null) {
            return "or";
        }
        res = i.get("OR");
        if (res != null) {
            return "OR";
        }

        return "";
    }

    public final static<T>  String mappingValue(Map<String, T> i) {
        try {
            if (i.size() != 1) {
                //todo 日志
                return "";
            } else {
                return i.values().iterator().next().toString();
            }
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }
    public final static<T> String mappingKey(Map<String, T> i) {
        if (i.size() != 1) {
            //todo 日志
            return "";
        } else {
            return i.keySet().iterator().next().toString();
        }
    }
}
