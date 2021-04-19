package io.mycat.calcite.spm;

import lombok.SneakyThrows;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

interface PlanManagerPersistor {

    void checkStore();

    Optional<Baseline> loadBaseline(long baseline);

    void deleteBaseline(long baseline);

    void saveBaselines(Baseline baseline);

    Map<Constraint,Baseline> loadAllBaseline();

    public void saveBaselines(Collection<Baseline> baselines);

    List<BaselinePlan> listPlan(long baseline);

    public Optional<BaselinePlan> loadPlan(long planId);

    public void savePlan(BaselinePlan plan,boolean fix);

    public void deletePlan(long planId);

    Optional<Baseline> loadBaselineByBaseLineSql(String baseLineSql,Constraint constraint );

    void deleteBaselineByExtraConstraint(List<String> infos);
}
