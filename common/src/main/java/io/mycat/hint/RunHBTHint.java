package io.mycat.hint;

import io.mycat.util.JsonUtil;

import java.text.MessageFormat;
import java.util.Collections;

public class RunHBTHint extends HintBuilder {
    private String hbt;

    public static String create(String hbt) {
        RunHBTHint createDataSourceHint = new RunHBTHint();
        createDataSourceHint.setHbt(hbt);
        return createDataSourceHint.build();
    }

    public void setHbt(String hbt) {
        this.hbt = hbt;
    }

    @Override
    public String getCmd() {
        return "run";
    }

    @Override
    public String build() {
        return MessageFormat.format("/*+ mycat:{0}{1} */;",
                getCmd(),
                JsonUtil.toJson(Collections.singletonMap("hbt", hbt)));
    }
}