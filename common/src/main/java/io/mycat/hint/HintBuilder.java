package io.mycat.hint;

import io.mycat.util.JsonUtil;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

public  abstract class HintBuilder {
        final Map<String, Object> map = new HashMap<>();

        public String build() {
            return MessageFormat.format("/*+ mycat:{0}{1} */;",
                    getCmd(),
                    JsonUtil.toJson(map));
        }

        public abstract String getCmd();
    }