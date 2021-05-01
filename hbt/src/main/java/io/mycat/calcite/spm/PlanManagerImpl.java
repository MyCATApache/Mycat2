//package io.mycat.calcite.spm;
//
//import java.util.*;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.stream.Collectors;
//
//public class PlanManagerImpl {
//
//    private final PlanManagerPersistor persistor;
//
//    public PlanManagerImpl(PlanManagerPersistor persistor) {
//        this.persistor = persistor;
//        this.persistor.loadAllBaseline();
//    }
//
//    public Optional<Baseline> load(long baselineId) {
//        return persistor.loadBaseline(baselineId).map(baseline1 -> {
//            map.put(baseline1.getBaselineId(), baseline1);
//            return baseline1;
//        });
//    }
//
//    public Optional<BaselinePlan> loadPlan(long planId) {
//        return persistor.loadPlan(planId).map(plan -> {
//            Baseline baseline = map.get(plan.getBaselineId());
//            if (baseline != null) {
//                baseline.getPlanList().add(plan);
//            }
//            return plan;
//        });
//    }
//
//    public List<BaselinePlan> list(long baseline) {
//        List<BaselinePlan> list = new LinkedList<>(persistor.listPlan(baseline));
//        Baseline curBaseline = map.get(baseline);
//        if (curBaseline != null) {
//            list.addAll(curBaseline.getPlanList());
//        }
//        return list.stream().distinct().sorted((o1, o2) -> (int) (o1.id - o2.id)).collect(Collectors.toList());
//    }
//
//
//    public void persist(Baseline baseline) {
//        persistor.saveBaseline(baseline);
//    }
//
//
//    public void persistPlan(BaselinePlan plan) {
//        persistor.savePlan(plan);
//    }
//
//
//    public void clear(long baseline) {
//        map.remove(baseline);
//    }
//
//
//    public void clearPlan(long planId) {
//        for (Baseline value : map.values()) {
//            value.removePlanById(planId);
//        }
//    }
//
//
//    public void delete(long baseline) {
//        persistor.deleteBaseline(baseline);
//    }
//
//
//    public void deletePlan(long planId) {
//        persistor.deletePlan(planId);
//    }
//
//    public void fixBaselinePlan(BaselinePlan plan) {
//        persistPlan(plan.changeToAccept(true));
//    }
//
//    public Optional<Baseline> loadByBaselineSql(String baseLineSql) {
//        return persistor.loadBaselineByBaseLineSql(baseLineSql);
//    }
//}
//
