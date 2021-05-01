package io.mycat.calcite.spm;

import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;

import java.util.List;
import java.util.Map;

public interface PlanManager {

    public String add(SQLSelectStatement sqlSelectStatement);
    public String fix(SQLSelectStatement sqlSelectStatement);
    public void load(long baseline);
    public void loadPlan(long planId);
    public List<BaselinePlan> list(long baseline);
    public void persist(long baseline);
    public void persistPlan(long planId);
    public void clear(long baseline);
    public void clearPlan(long planId);
    public void delete(long baseline);
    public void deletePlan(long planId);

}
