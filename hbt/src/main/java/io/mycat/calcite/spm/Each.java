package io.mycat.calcite.spm;


import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
@ToString
@Builder
public class Each implements Comparable<Each> {
    String targetName;
    String sql;

    public Each(String targetName, String sql) {
        this.targetName = targetName;
        this.sql = sql.replaceAll("\n", " ").replaceAll("\r", " ");
    }

    public static Each of(String targetName, String sql) {
        return new Each(targetName, sql);
    }

    @Override
    public int compareTo(Each o) {
        int i = this.targetName.compareTo(o.targetName);
        if (i == 0) {
            return this.sql.compareTo(o.sql);
        }
        return i;
    }
}