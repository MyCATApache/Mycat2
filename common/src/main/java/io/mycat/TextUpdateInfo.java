package io.mycat;

import java.util.List;

public interface TextUpdateInfo {
    public static TextUpdateInfo create(String targetName, List<String> sqls) {
        return new TextUpdateInfoImpl(targetName, sqls);
    }

    String targetName();

    List<String> sqls();

    public class TextUpdateInfoImpl implements TextUpdateInfo {
        final String targetName;
        final List<String> sqls;

        public TextUpdateInfoImpl(String targetName, List<String> sqls) {
            this.targetName = targetName;
            this.sqls = sqls;
        }


        @Override
        public String targetName() {
            return targetName;
        }

        @Override
        public List<String> sqls() {
            return sqls;
        }
    }
}
