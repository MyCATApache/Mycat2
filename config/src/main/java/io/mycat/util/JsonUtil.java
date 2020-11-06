package io.mycat.util;


import com.alibaba.fastjson.JSON;

import java.util.Map;
import java.util.Optional;

public class JsonUtil {

    public static <T> T from(String text, Class<T> clazz) {
        return (T) JSON.parseObject(text, clazz);
    }
    public static Optional<String> fromMap(String text, String item) {
        if (text == null){
            return Optional.empty();
        }
        try {
            Map map = JSON.parseObject(text, Map.class);
          return   Optional.ofNullable(map).map(i->(String)i.get(item));
        }catch (Throwable t){
            return Optional.empty();
        }
    }

    public static String toJson(Object o) {
        return JSON.toJSONString(o);
    }
}