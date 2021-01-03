package io.mycat.util;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class JsonUtil {
    public static Map<String, String> urlSplit(String s){
        Map<String, String> mapRequest = new HashMap<>(2);
        String strUrlParam = truncateUrlPage(s);
        if(strUrlParam == null){
            return mapRequest;
        }
        String[] arrSplit = strUrlParam.split("[&]");
        for(String strSplit : arrSplit){
            String[] arrSplitEqual = strSplit.split("[=]");
            //解析出键值
            if(arrSplitEqual.length>1){
                //正确解析
                mapRequest.put(arrSplitEqual[0], arrSplitEqual[1]);
            }else{
                if(!"".equals(arrSplitEqual[0])){
                    //只有参数没有值，不加入
                    mapRequest.put(arrSplitEqual[0], "");
                }
            }
        }
        return mapRequest;
    }

    private static String truncateUrlPage(String strURL){
        String strAllParam=null;
        strURL = strURL.trim();
        String[] arrSplit = strURL.split("[?]");
        if(strURL.length() > 1){
            if(arrSplit.length > 1){
                for (int i=1; i < arrSplit.length; i++){
                    strAllParam = arrSplit[i];
                }
            }
        }
        return strAllParam;
    }

    public static <T> T from(String text, Class<T> clazz) {
        return (T) JSON.parseObject(text, clazz);
    }

    public static Optional<String> fromMap(String text, String item) {
        if (text == null) {
            return Optional.empty();
        }
        try {
            Map map = JSON.parseObject(text, Map.class);
            return Optional.ofNullable(map).map(i -> (String) i.get(item));
        } catch (Throwable t) {
            return Optional.empty();
        }
    }

    public static String toJson(Object o) {
        return JSON.toJSONString(o, SerializerFeature.PrettyFormat);
    }
}