package io.mycat.calcite.spm;

import com.alibaba.druid.util.JdbcUtils;
import io.mycat.MetaClusterCurrent;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.util.JsonUtil;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DbPlanManagerPersistorImpl implements PlanManagerPersistor {

    final String datasourceName = "prototype";
    final static Logger log = LoggerFactory.getLogger(DbPlanManagerPersistorImpl.class);


    @SneakyThrows
    public DbPlanManagerPersistorImpl() {
    }


    /**
     * SHOW CREATE TABLE mycat.`spm_baseline`;
     * SHOW CREATE TABLE mycat.`spm_plan`;
     */
    @Override
    @SneakyThrows
    public synchronized void checkStore() {
        try (DefaultConnection connection = getManager().getConnection(datasourceName);) {
            JdbcUtils.execute(connection.getRawConnection(), "CREATE DATABASE  IF  NOT EXISTS mycat");
            JdbcUtils.execute(connection.getRawConnection(), "CREATE TABLE IF  NOT EXISTS mycat.`spm_baseline` (\n" +
                    "  `id` bigint(22) NOT NULL AUTO_INCREMENT,\n" +
                    "  `fix_plan_id` bigint(22) DEFAULT NULL,\n" +
                    "  `constraint` longtext CHARACTER SET utf8mb4 NOT NULL,\n" +
                    "  `extra_constraint` longtext,\n" +
                    "  PRIMARY KEY (`id`),\n" +
                    "  UNIQUE KEY `constraint_index` (`constraint`(22)),\n" +
                    "  KEY `id` (`id`)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ");
            JdbcUtils.execute(connection.getRawConnection(), "CREATE TABLE IF  NOT EXISTS mycat.`spm_plan` (\n" +
                    "  `id` bigint(22) NOT NULL AUTO_INCREMENT,\n" +
                    "  `sql` longtext,\n" +
                    "  `rel` longtext,\n" +
                    "  `baseline_id` bigint(22) DEFAULT NULL,\n" +
                    "  KEY `id` (`id`)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
        }
    }

    @Override
    @SneakyThrows
    public synchronized Optional<Baseline> loadBaseline(long baselineId) {
        try (DefaultConnection connection = getManager().getConnection(datasourceName);) {
            List<Map<String, Object>> maps = JdbcUtils.executeQuery(connection.getRawConnection(), "SELECT * FROM mycat.spm_baseline where id = ?", Arrays.asList(baselineId));
            if (maps.isEmpty()) {
                return Optional.empty();
            }
            if (maps.size() != 1) {
                log.error("baseline is duplicate");
                return Optional.empty();
            }
            Map<String, Object> map = maps.get(0);
            Baseline baseline = toBaseline(map);
            return Optional.of(baseline);
        }
    }

    @NotNull
    private Baseline toBaseline(Map<String, Object> map) {
        return toBaseline(map, (id,baselineId) -> loadFixPlan(id,baselineId));
    }

    private Baseline toBaseline(Map<String, Object> map, BiFunction<Long,Long, Optional<BaselinePlan>> fetchBaselinePlanFunction) {
        Long fix_plan_id = (Long) map.get("fix_plan_id");
        Long id = (Long) map.get("id");
        Optional<BaselinePlan> baselinePlanOptional;
        if (fix_plan_id != null) {
            baselinePlanOptional = fetchBaselinePlanFunction.apply(fix_plan_id,id);
        } else {
            baselinePlanOptional = Optional.empty();
        }

        String constraintText = (String) map.get("constraint");
        String extraConstraintText = (String) map.get("extra_constraint");
        List<BaselinePlan> list = listPlan(id);
        Constraint constraint = JsonUtil.from(constraintText, Constraint.class);
        ExtraConstraint extraConstraint = JsonUtil.from(extraConstraintText, ExtraConstraint.class);
        Baseline baseline = new Baseline(id, (String) constraint.getSql(), constraint, baselinePlanOptional.orElse(null), extraConstraint);
        baseline.getPlanList().addAll(list);
        return baseline;
    }

    @Override
    @SneakyThrows
    public synchronized void deleteBaseline(long baseline) {
        try (DefaultConnection connection = getManager().getConnection(datasourceName);) {
            JdbcUtils.executeUpdate(connection.getRawConnection(), "delete  FROM mycat.spm_baseline where id = ?", Arrays.asList(baseline));
            JdbcUtils.executeUpdate(connection.getRawConnection(), "delete  FROM mycat.spm_plan where baseline_id = ?", Arrays.asList(baseline));
        }
    }

    @Override
    @SneakyThrows
    public synchronized List<BaselinePlan> listPlan(long baseline) {
        try (DefaultConnection connection = getManager().getConnection(datasourceName);) {
            List<Map<String, Object>> maps = JdbcUtils.executeQuery(connection.getRawConnection(), "select *  FROM mycat.spm_plan where baseline_id = ?", Arrays.asList(baseline));
            return maps.stream().map((Function<Map<String, Object>, BaselinePlan>) map -> {
                return new BaselinePlan((String) map.get("sql"), (String) map.get("rel"), (Long) map.get("id"), (Long) map.get("baseline_id"), null);
            }).collect(Collectors.toList());

        }
    }

    @Override
    @SneakyThrows
    public synchronized void saveBaseline(Baseline baseline) {
//        String constraintText = JsonUtil.toJson(baseline.getConstraint());
//        try (DefaultConnection connection = getManager().getConnection(datasourceName);) {
//            Connection rawConnection = connection.getRawConnection();
//            rawConnection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
//            rawConnection.setAutoCommit(false);
//            List<Map<String, Object>> maps = JdbcUtils.executeQuery(rawConnection, "select * from mycat.spm_baseline where `constraint` = ?",
//                    Arrays.asList(constraintText));
//            if (maps.isEmpty()) {
//                insertBaseline(baseline, rawConnection);
//            }
//            rawConnection.commit();
//        }
       saveBaseline(Collections.singleton(baseline));
    }

    private void insertBaseline(Baseline baseline, Connection rawConnection) throws SQLException {
        JdbcUtils.execute(rawConnection, "INSERT INTO  mycat.spm_baseline (id,`constraint`,`extra_constraint`,`fix_plan_id`) values(?,?,?,?)  on duplicate key update `constraint` = VALUES(`constraint`)",
                Arrays.asList(baseline.getBaselineId(),
                        JsonUtil.toJson(baseline.getConstraint()),
                        JsonUtil.toJson(baseline.getExtraConstraint()),
                        Optional.ofNullable(baseline.fixPlan).map(i -> i.getId()).orElse(null)));
    }

    @Override
    @SneakyThrows
    public synchronized Map<Constraint, Baseline> loadAllBaseline() {
        try (DefaultConnection connection = getManager().getConnection(datasourceName);) {
            Connection rawConnection = connection.getRawConnection();
            Map<Constraint, Baseline> baselineList = JdbcUtils.executeQuery(rawConnection, "SELECT * FROM mycat.spm_baseline", Collections.emptyList())
                    .stream().map(b -> toBaseline(b)).distinct().collect(Collectors.toMap(k -> k.getConstraint(), v -> v));
            if (baselineList.isEmpty()) {
                return Collections.emptyMap();
            }
            Map<Long, List<BaselinePlan>> baselinePlanMap = JdbcUtils.executeQuery(connection.getRawConnection(), "SELECT * FROM mycat.spm_plan", Collections.emptyList())
                    .stream().parallel().map(m -> toBaselinePlan(m)).collect(Collectors.groupingBy(k -> k.getBaselineId()));
            for (Baseline baseline : baselineList.values()) {
                List<BaselinePlan> baselinePlans = baselinePlanMap.getOrDefault(baseline.baselineId, Collections.emptyList());
                baseline.getPlanList().addAll(baselinePlans);
            }
            return baselineList;
        }
    }

    @Override
    @SneakyThrows
    public void saveBaseline(Collection<Baseline> baselines) {
        try (DefaultConnection connection = getManager().getConnection(datasourceName);) {
            Connection rawConnection = connection.getRawConnection();
            rawConnection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            rawConnection.setAutoCommit(false);


            try (PreparedStatement preparedStatement = rawConnection
                    .prepareStatement(
                            "INSERT INTO  mycat.spm_baseline (id,`constraint`,`extra_constraint`,`fix_plan_id`) values(?,?,?,?) " +
                                    "on duplicate key update id = values(id),`constraint` = values(`constraint`),`extra_constraint`=values(`extra_constraint`),`fix_plan_id`=values(`fix_plan_id`)");) {
                for (Baseline baseline : baselines) {
                    long baselineId = baseline.getBaselineId();
                    String constraintText = JsonUtil.toJson(baseline.getConstraint());
                    String extraConstraintText = JsonUtil.toJson(baseline.getExtraConstraint());

                    preparedStatement.setObject(1, baselineId);
                    preparedStatement.setObject(2, constraintText);
                    preparedStatement.setObject(3, extraConstraintText);
                    preparedStatement.setObject(4, Optional.ofNullable(baseline.fixPlan).map(i -> i.getId()).orElse(null));
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
            }
            try (PreparedStatement preparedStatement = rawConnection
                    .prepareStatement("INSERT INTO  mycat.spm_plan (id,`sql`,`baseline_id`,`rel`) values(?,?,?,?) " +
                            "on duplicate key update `id` = values(`id`),`sql` = values(`sql`),`baseline_id` = values(`baseline_id`),`rel` = values(`rel`)");) {
                for (Baseline baseline : baselines) {
                    for (BaselinePlan baselinePlan : baseline.getPlanList()) {

                            preparedStatement.setObject(1, baselinePlan.getId());
                            preparedStatement.setObject(2, baselinePlan.getSql());
                            preparedStatement.setObject(3, baseline.getBaselineId());
                            preparedStatement.setObject(4, baselinePlan.getRel());
                            preparedStatement.addBatch();

                    }
                }
                preparedStatement.executeBatch();
            }
            rawConnection.commit();
        }
    }

    @SneakyThrows
    public synchronized void clear() {
        try (DefaultConnection connection = getManager().getConnection(datasourceName);) {
            JdbcUtils.execute(connection.getRawConnection(), "truncate mycat.spm_plan", Arrays.asList());
            JdbcUtils.execute(connection.getRawConnection(), "truncate mycat.spm_baseline", Arrays.asList());
        }
    }

    @SneakyThrows
    public synchronized Optional<BaselinePlan> loadPlan(long planId) {
        try (DefaultConnection connection = getManager().getConnection(datasourceName);) {
            List<Map<String, Object>> maps = JdbcUtils.executeQuery(connection.getRawConnection(), "SELECT * FROM mycat.spm_plan where id = ? and baseline_id = ?", Arrays.asList(planId));
            if (maps.size() != 1) {
                log.error("baseline is duplicate");
                return Optional.empty();
            }
            Map<String, Object> map = maps.get(0);
            return Optional.ofNullable(toBaselinePlan(map));
        }
    }
    @SneakyThrows
    public synchronized Optional<BaselinePlan> loadFixPlan(long planId,long baselineId) {
        try (DefaultConnection connection = getManager().getConnection(datasourceName);) {
            List<Map<String, Object>> maps = JdbcUtils.executeQuery(connection.getRawConnection(), "SELECT * FROM mycat.spm_plan where id = ? and baseline_id = ?", Arrays.asList(planId,baselineId));
            if (maps.size() != 1) {
                log.error("baseline is duplicate");
                return Optional.empty();
            }
            Map<String, Object> map = maps.get(0);
            return Optional.ofNullable(toBaselinePlan(map));
        }
    }
    @SneakyThrows
    public synchronized List<BaselinePlan> loadPlanByBaselineId(long baselineId) {
        try (DefaultConnection connection = getManager().getConnection(datasourceName);) {
            List<Map<String, Object>> maps = JdbcUtils.executeQuery(connection.getRawConnection(), "SELECT * FROM mycat.spm_plan where baseline_id = ?", Arrays.asList(baselineId));
            return maps.stream().map(i->toBaselinePlan(i)).collect(Collectors.toList());
        }
    }
    @NotNull
    private BaselinePlan toBaselinePlan(Map<String, Object> map) {
        String sql = (String) map.get("sql");
        String rel = (String) map.get("rel");
        long baseline_id = (Long) map.get("baseline_id");
        long id = (Long) map.get("id");
        BaselinePlan baselinePlan = new BaselinePlan(sql, rel, id, baseline_id, null);
        return baselinePlan;
    }

    @SneakyThrows
    public synchronized void savePlan(BaselinePlan plan, boolean fix) {
        try (DefaultConnection connection = getManager().getConnection(datasourceName);) {
            Connection rawConnection = connection.getRawConnection();
            rawConnection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            rawConnection.setAutoCommit(false);
//            List<Map<String, Object>> maps = JdbcUtils.executeQuery(rawConnection, "select * from mycat.spm_plan where `baseline_id` = ? and `rel` =  ?",
//                    Arrays.asList(plan.getBaselineId(), plan.getRel()));
//            if (maps.isEmpty()) {
                JdbcUtils.execute(rawConnection, "replace mycat.spm_plan (id,`sql`,`baseline_id`,`rel`) values(?,?,?,?)",
                        Arrays.asList(plan.getId(), plan.getSql(), plan.getBaselineId(), plan.rel));
//            }
            if (fix) {
                JdbcUtils.execute(rawConnection, "update mycat.spm_baseline set fix_plan_id  = ? where id = ?",
                        Arrays.asList(plan.getBaselineId(), plan.getId()));
            }
            rawConnection.commit();
        }
    }

    @Override
    @SneakyThrows
    public synchronized void deletePlan(long planId) {
        try (DefaultConnection connection = getManager().getConnection(datasourceName);) {
            Connection rawConnection = connection.getRawConnection();
            JdbcUtils.execute(rawConnection, "delete from mycat.spm_plan where id = ?", Arrays.asList(planId));
            JdbcUtils.execute(rawConnection, "update  mycat.spm_baseline set  fix_plan_id = null where fix_plan_id = ?", Arrays.asList(planId));
        }
    }

    @Override
    @SneakyThrows
    public synchronized Optional<Baseline> loadBaselineByBaseLineSql(String baseLineSql, Constraint constraint) {
        try (DefaultConnection connection = getManager().getConnection(datasourceName);) {
            Connection rawConnection = connection.getRawConnection();
            List<Map<String, Object>> maps = JdbcUtils.executeQuery(rawConnection, "select * from mycat.spm_baseline where `constraint` = ?", Arrays.asList(JsonUtil.toJson(constraint)));
            if (maps.isEmpty()) {
                return Optional.empty();
            }
            Map<String, Object> map = maps.get(0);
            return Optional.ofNullable(toBaseline(map));
        }
    }

    @Override
    @SneakyThrows
    public void deleteBaselineByExtraConstraint(List<String> infos) {
        if (infos.isEmpty()) {
            return;
        }
        try (DefaultConnection connection = getManager().getConnection(datasourceName);) {
            Connection rawConnection = connection.getRawConnection();
            List<String> list = new ArrayList<>();
            for (String info : infos) {
                list.add(" extra_constraint like \"%" + info + "%\" ");
            }
            JdbcUtils.executeQuery(rawConnection, "delete  from mycat.spm_baseline where " + String.join(" or ", list), Collections.emptyList());
        }
    }

    public JdbcConnectionManager getManager() {
        return MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
    }
}
