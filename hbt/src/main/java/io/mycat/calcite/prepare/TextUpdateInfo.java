package io.mycat.calcite.prepare;

import io.mycat.hbt.ast.modify.MergeModify;
import io.mycat.hbt.ast.modify.ModifyFromSql;

import java.util.List;

public interface TextUpdateInfo {
    String targetName();

    List<String> sqls();

    public static TextUpdateInfo create(String targetName, List<String> sqls) {
        return new TextUpdateInfoImpl(targetName, sqls);
    }

    default MergeModify convertToModifyFromSql() {
        String s = targetName();
        return new MergeModify(() -> sqls().stream().map(i -> new ModifyFromSql(s, i)).iterator());
    }

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
