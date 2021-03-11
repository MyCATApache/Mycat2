package io.mycat.calcite.spm;


import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Arrays;
import java.util.List;


@EqualsAndHashCode
@ToString
@Builder
public class SpecificSql implements Comparable<SpecificSql> {
    String relNode;
    String parameterizedSql;
    List<Each> sqls;

    public SpecificSql(String relNode, String parameterizedSql, List<Each> sqls) {
        this.relNode = relNode.replaceAll("\n", " ").replaceAll("\r"," ");
        this.parameterizedSql = parameterizedSql.replaceAll("\n", " ").replaceAll("\r"," ");
        this.sqls = sqls;
    }
    public static SpecificSql of (String relNode, String parameterizedSql, Each... sqls) {
        return new SpecificSql(relNode, parameterizedSql, Arrays.asList(sqls));
    }

    @Override
    public int compareTo(SpecificSql o) {
        return this.parameterizedSql.compareTo(o.parameterizedSql);
    }
}