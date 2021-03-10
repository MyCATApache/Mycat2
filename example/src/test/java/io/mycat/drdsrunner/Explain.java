package io.mycat.drdsrunner;

import io.mycat.calcite.spm.Plan;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Util;

import java.util.List;


@Getter
public class Explain {
    Plan plan;
    List<String> lines;

    public Explain(Plan plan, List<String> lines) {
        this.plan = plan;
        this.lines = lines;
    }

    public String getColumnInfo() {
        return plan.getMetaData().toSimpleText();
    }

    public String dumpPlan2() {
        String dumpPlan = Util.toLinux(RelOptUtil.dumpPlan("", plan.getPhysical(), SqlExplainFormat.TEXT,
                SqlExplainLevel.EXPPLAN_ATTRIBUTES));
        System.out.println(dumpPlan);
        return dumpPlan;
    }
    public String dumpPlan() {
        return String.join("\n",lines);
    }
}