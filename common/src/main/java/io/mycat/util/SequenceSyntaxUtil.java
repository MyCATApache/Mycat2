package io.mycat.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SequenceSyntaxUtil {
    static final Pattern pattern = Pattern.compile("(?:(\\s*next\\s+value\\s+for\\s*MYCATSEQ_(\\w+))(,|\\)|\\s)*)+", Pattern.CASE_INSENSITIVE);

    public static String rewrite(String executeSql) {
        Matcher matcher = pattern.matcher(executeSql);
        while (matcher.find()) {
            String group = matcher.group(2);
            executeSql = executeSql.replaceFirst(matcher.group(1), "next_value_for('"+group+"')");
        }
        return executeSql;
    }

    public static void main(String[] args) {
        String s = "insert into table1(id,name) values(next value for MYCATSEQ_GLOBAL,‘test’);";
        String rewrite = SequenceSyntaxUtil.rewrite(s);
    }
}