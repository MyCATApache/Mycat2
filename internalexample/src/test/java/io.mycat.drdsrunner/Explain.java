package io.mycat.drdsrunner;

import io.mycat.DrdsSql;
import io.mycat.DrdsSqlWithParams;
import io.mycat.calcite.spm.Plan;
import io.mycat.calcite.spm.SpecificSql;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Util;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.stream.Collectors;


@Getter
public class Explain {
    private Plan plan;
    private DrdsSqlWithParams drdsSql;

    public Explain(Plan plan, DrdsSqlWithParams drdsSql) {
        this.plan = plan;
        this.drdsSql = drdsSql;
    }

    public String getColumnInfo() {
        return plan.getMetaData().toSimpleText();
    }

//    public String dumpPlan2() {
//        String dumpPlan = Util.toLinux(RelOptUtil.dumpPlan("", plan.getPhysical(), SqlExplainFormat.TEXT,
//                SqlExplainLevel.EXPPLAN_ATTRIBUTES));
//        System.out.println(dumpPlan);
//        return dumpPlan;
//    }
    public String dumpPlan() {
        return plan.dumpPlan().replaceAll("\r"," ").replaceAll("\n"," ").trim();
    }
//    public  List<SpecificSql> specificSql() {
//        return plan.specificSql(drdsSql);
//    }
@NotNull
public List<SpecificSql> specificSql(){
        return plan.specificSql(drdsSql);
}

}