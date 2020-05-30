package io.mycat.mpp.expr.func;

import com.alibaba.fastsql.util.DataTimeUtils;
import io.mycat.mpp.NullPointer;

import java.nio.charset.Charset;
import java.time.LocalDateTime;

public class CastOps {


    /////////////////////////////////cast_as_char////////////////////////////
    public static String cast_as_char(String p, int lengthArg, Charset charset) {
        return cast_as_char(p, lengthArg, charset, NullPointer.DEFAULT);
    }

    public static String cast_as_char(String p, int lengthArg, Charset charset, NullPointer nullPointer) {
        if (p == null) {
            nullPointer.setNullValue(true);
            return null;
        }
        nullPointer.setNullValue(false);
        if (lengthArg < 0) {
            lengthArg = p.length();
        }
        if (lengthArg < p.length()) {
            p = p.substring(0, lengthArg);
        }
       return new String(p.getBytes(), charset);
    }

}