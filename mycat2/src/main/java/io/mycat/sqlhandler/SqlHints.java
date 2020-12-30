package io.mycat.sqlhandler;

import com.alibaba.druid.sql.ast.SQLStatement;
import io.mycat.util.StringUtil;

import java.util.Collections;
import java.util.Map;

public class SqlHints {
    public static final String AFTER_COMMENT = "rowFormat.after_comment";
    public static final String BEFORE_COMMENT = "rowFormat.before_comment";

    public static String unWrapperHint(String hint) {
        if (hint == null) {
            return null;
        }
        String text = hint.trim();
        if (text.startsWith("/* !")) {
            text = text.substring(2);
        }
        if (text.startsWith("/*!")) {
            text = text.substring(3);
        }
        if (text.startsWith("/*")) {
            text = text.substring(2);
        }
        if (text.startsWith("!")){
            text = text.substring(1);
        }
        /////////////////////////////////////////////////////
        if (text.startsWith("/* +")) {
            text = text.substring(2);
        }
        if (text.startsWith("/*")) {
            text = text.substring(3);
        }
        if (text.startsWith("/*")) {
            text = text.substring(2);
        }
        if (text.startsWith("+")){
            text = text.substring(1);
        }
        return text.trim();
    }

    public static boolean isJson(String text) {
        return text.startsWith("{")&&text.endsWith("}");
    }
}
