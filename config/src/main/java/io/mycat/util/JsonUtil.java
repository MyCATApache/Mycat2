package io.mycat.util;


import com.alibaba.fastjson.JSON;

public class JsonUtil {

    public static <T> T from(String text, Class<T> clazz) {
        return (T) JSON.parseObject(text, clazz);
    }

    public static String toJson(Object o) {
        return JSON.toJSONString(o);
    }
}