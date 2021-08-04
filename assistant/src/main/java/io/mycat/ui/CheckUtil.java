package io.mycat.ui;


import com.sun.xml.internal.messaging.saaj.packaging.mime.MessagingException;
import org.apache.commons.lang3.StringUtils;

public class CheckUtil {
    public static String isEmpty(String text, String message) {
        if (StringUtils.isEmpty(text)){
            throw new IllegalArgumentException(message);
        }
        return text;
    }
}
