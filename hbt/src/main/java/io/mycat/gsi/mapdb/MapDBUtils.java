package io.mycat.gsi.mapdb;

import com.alibaba.fastjson.parser.ParserConfig;
import com.alibaba.fastjson.util.TypeUtils;

public class MapDBUtils {

    public static Object cast(Object value,Class javaClass){
        Object result = TypeUtils.cast(value, javaClass, ParserConfig.getGlobalInstance());
        return result;
    }
}
