package io.mycat;

import cn.mycat.vertx.xa.XaSqlConnection;
import com.alibaba.druid.sql.ast.SQLStatement;
import io.mycat.beans.mycat.CopyMycatRowMetaData;
import io.mycat.beans.mysql.MySQLErrorCode;
import io.mycat.beans.mysql.MySQLType;
import io.mycat.calcite.*;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.logical.MycatViewDataNodeMapping;
import io.mycat.calcite.logical.SqlTemplate;
import io.mycat.calcite.physical.MycatInsertRel;
import io.mycat.calcite.physical.MycatMergeSort;
import io.mycat.calcite.physical.MycatUpdateRel;
import io.mycat.calcite.plan.ObservablePlanImplementorImpl;
import io.mycat.calcite.plan.PlanImplementor;
import io.mycat.calcite.resultset.CalciteRowMetaData;
import io.mycat.calcite.spm.Plan;
import io.mycat.calcite.spm.PlanImpl;
import io.mycat.calcite.table.MycatTransientSQLTableScan;
import io.mycat.util.VertxUtil;
import io.mycat.vertx.MycatTransientSQLTableScanMappingFunction;
import io.mycat.vertx.ShardingTableDataNodeMapping;
import io.vertx.core.Future;
import lombok.SneakyThrows;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.tree.ClassDeclaration;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.util.*;

public class DrdsExecutorCompiler {
    private final static Logger log = LoggerFactory.getLogger(DrdsExecutorCompiler.class);

    public static SqlTypeName getObjectType(Object value) {
        if (value == null) {
            return (SqlTypeName.NULL);
        } else {
            Class<?> aClass = value.getClass();
            MySQLType[] mySQLTypes = MySQLType.values();
            for (MySQLType e : mySQLTypes) {
                if (e.getJavaClass() == aClass) {
                    return (SqlTypeName.getNameForJdbcType(e.getJdbcType()));
                }
                if (Integer.class == aClass) {
                    return SqlTypeName.INTEGER;
                }
                if (byte[].class == aClass) {
                    return SqlTypeName.BINARY;
                }
            }
            throw new IllegalArgumentException("unknown type :" + aClass);
        }
    }

    private static List<Object> normalize(List<SqlTypeName> targetTypes, List<Object> params) {
        int size = targetTypes.size();
        if (targetTypes.isEmpty()) {
            return Collections.emptyList();
        }
        if (params.get(0) instanceof List) {
            return params;//insert or update sql
        }
        List<Object> resParams = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            Object param = params.get(i);
            SqlTypeName targetTypeName = targetTypes.get(i);
            SqlTypeName curTypeName = getObjectType(param);
            Object curParam = params.get(i);
            if (targetTypeName == curTypeName || curParam == null) {
                resParams.add(curParam);
            } else {
                switch (targetTypeName) {
                    case BOOLEAN:
                        if (curParam instanceof Boolean) {
                            curParam = curParam;
                        } else if (curParam instanceof String) {
                            curParam = Boolean.parseBoolean((String) curParam);
                        } else if (curParam instanceof Number) {
                            long l = ((Number) curParam).longValue();
                            curParam = (l == -1 || l > 0);
                        }
                        throw new UnsupportedOperationException();
                    case TINYINT:
                        if (curParam instanceof Number) {
                            curParam = ((Number) curParam).byteValue();
                        } else if (curParam instanceof String) {
                            curParam = Byte.parseByte((String) curParam);
                        } else {
                            throw new UnsupportedOperationException();
                        }
                        break;
                    case SMALLINT:
                        if (curParam instanceof Number) {
                            curParam = ((Number) curParam).shortValue();
                        } else if (curParam instanceof String) {
                            curParam = Short.parseShort((String) curParam);
                        } else {
                            throw new UnsupportedOperationException();
                        }
                        break;
                    case INTEGER:
                        if (curParam instanceof Number) {
                            curParam = ((Number) curParam).intValue();
                        } else if (curParam instanceof String) {
                            curParam = Integer.parseInt((String) curParam);
                        } else {
                            throw new UnsupportedOperationException();
                        }
                        break;
                    case BIGINT:
                        if (curParam instanceof Number) {
                            curParam = ((Number) curParam).longValue();
                        } else if (curParam instanceof String) {
                            curParam = Long.parseLong((String) curParam);
                        } else {
                            throw new UnsupportedOperationException();
                        }
                        break;
                    case DECIMAL:
                        if (curParam instanceof Number) {
                            curParam = new BigDecimal(curParam.toString());
                        } else if (curParam instanceof String) {
                            curParam = new BigDecimal(curParam.toString());
                        } else {
                            throw new UnsupportedOperationException();
                        }
                        break;
                    case FLOAT:
                        if (curParam instanceof Float) {
                            curParam = ((Number) curParam).floatValue();
                        } else if (curParam instanceof String) {
                            curParam = Float.parseFloat(curParam.toString());
                        } else {
                            throw new UnsupportedOperationException();
                        }
                        break;
                    case REAL:
                    case DOUBLE:
                        if (curParam instanceof Number) {
                            curParam = ((Number) curParam).doubleValue();
                        } else if (curParam instanceof String) {
                            curParam = Double.parseDouble(curParam.toString());
                        } else {
                            throw new UnsupportedOperationException();
                        }
                        break;
                    case DATE:
                        if (curParam instanceof Date) {
                            curParam = new java.sql.Date(((Date) curParam).getTime()).toLocalDate();
                        } else if (curParam instanceof String) {
                            curParam = java.sql.Date.valueOf((String) curParam);
                        } else if (curParam instanceof LocalDate) {
                            curParam = curParam;
                        } else {
                            throw new UnsupportedOperationException();
                        }
                        break;
                    case TIME_WITH_LOCAL_TIME_ZONE:
                    case TIME:
                        if (curParam instanceof Duration) {
                            curParam = curParam;
                        } else {
                            curParam = curParam.toString();
                        }
                        curParam = MycatTimeUtil.timeStringToTimeDuration(curParam.toString());
                        break;
                    case TIMESTAMP:
                    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                        if (curParam instanceof Timestamp) {
                            curParam = ((Timestamp) curParam).toLocalDateTime();
                        } else if (curParam instanceof String) {
                            curParam = Timestamp.valueOf((String) curParam).toLocalDateTime();
                        }
                    {
                        curParam = curParam;
                    }
                    break;
                    case CHAR:
                        curParam = curParam.toString();
                        break;
                    case VARCHAR:
                        curParam = curParam.toString();
                        break;
                    case BINARY:
                    case VARBINARY:
                        if (curParam instanceof byte[]) {
                            curParam = new ByteString((byte[]) curParam);
                        } else {
                            curParam = curParam;
                        }
                        break;
                    case NULL:
                        curParam = null;
                        break;
                    case ANY:
                    case SYMBOL:
                    case MULTISET:
                    case ARRAY:
                    case MAP:
                    case DISTINCT:
                    case STRUCTURED:
                    case ROW:
                    case OTHER:
                    case CURSOR:
                    case COLUMN_LIST:
                    case DYNAMIC_STAR:
                    case GEOMETRY:
                    case SARG:
                        curParam = curParam.toString();
                        break;
                    case INTERVAL_YEAR:
                    case INTERVAL_YEAR_MONTH:
                    case INTERVAL_MONTH:
                    case INTERVAL_DAY:
                    case INTERVAL_DAY_HOUR:
                    case INTERVAL_DAY_MINUTE:
                    case INTERVAL_DAY_SECOND:
                    case INTERVAL_HOUR:
                    case INTERVAL_HOUR_MINUTE:
                    case INTERVAL_HOUR_SECOND:
                    case INTERVAL_MINUTE:
                    case INTERVAL_MINUTE_SECOND:
                    case INTERVAL_SECOND:
                    case INTERVAL_MICROSECOND:
                    case INTERVAL_WEEK:
                    case INTERVAL_QUARTER:
                    case INTERVAL_SECOND_MICROSECOND:
                    case INTERVAL_MINUTE_MICROSECOND:
                    case INTERVAL_HOUR_MICROSECOND:
                    case INTERVAL_DAY_MICROSECOND:
                        throw new UnsupportedOperationException();
                }
                resParams.add(curParam);
            }
        }
        return resParams;
    }

    public static List<SqlTypeName> getSqlTypeNames(List<Object> params) {
        ArrayList<SqlTypeName> list = new ArrayList<>();
        for (Object param : params) {
            if (param == null) {
                list.add(SqlTypeName.NULL);
            } else {
                Class<?> aClass = param.getClass();
                SqlTypeName sqlTypeName = null;
                MySQLType[] mySQLTypes = MySQLType.values();
                for (MySQLType value : mySQLTypes) {
                    if (value.getJavaClass() == aClass) {
                        sqlTypeName = (SqlTypeName.getNameForJdbcType(value.getJdbcType()));
                        break;
                    }
                    if (Integer.class == aClass) {
                        sqlTypeName = SqlTypeName.INTEGER;
                        break;
                    }
                    if (byte[].class == aClass) {
                        sqlTypeName = SqlTypeName.BINARY;
                        break;
                    }
                }
                list.add(Objects.requireNonNull(sqlTypeName, () -> "unknown type :" + param.getClass()));
            }
        }
        return list;
    }

    @NotNull
    @SneakyThrows
    public static CodeExecuterContext getCodeExecuterContext(MycatRel relNode, boolean forUpdate) {
        HashMap<String, Object> varContext = new HashMap<>(2);
        StreamMycatEnumerableRelImplementor mycatEnumerableRelImplementor = new StreamMycatEnumerableRelImplementor(varContext);
        RelContext relContext = new RelContext(relNode, forUpdate);
        relNode.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(RelNode other) {
                if (other instanceof MycatView) {
                    Rel rel = relContext.nodes.computeIfAbsent(other.getDigest(), s -> new Rel());
                    rel.count += 1;
                    MycatView mycatView = (MycatView) other;
                    visitMycatView(rel, mycatView);
                } else if (other instanceof MycatMergeSort) {
                    Rel rel = relContext.nodes.computeIfAbsent(other.getDigest(), s -> new Rel());
                    rel.count += 1;
                    MycatMergeSort mycatMergeSort = (MycatMergeSort) other;
                    visitMycatView(rel, (MycatView) mycatMergeSort.getInput());
                } else if (other instanceof MycatTransientSQLTableScan) {
                    Rel rel = relContext.nodes.computeIfAbsent(other.getDigest(), s -> new Rel());
                    rel.count += 1;
                    MycatTransientSQLTableScan transientSQLTableScan = (MycatTransientSQLTableScan) other;
                    visitTableScan(rel, transientSQLTableScan);
                }
                return super.visit(other);
            }

            private void visitTableScan(Rel rel, MycatTransientSQLTableScan transientSQLTableScan) {
                String sql = transientSQLTableScan.getSql();
                String targetName = transientSQLTableScan.getTargetName();
                rel.dataNodeMapping = new MycatTransientSQLTableScanMappingFunction(targetName, sql);
                rel.columnInfo = new CopyMycatRowMetaData(new CalciteRowMetaData(transientSQLTableScan.getRowType().getFieldList()));
            }

            private void visitMycatView(Rel rel, MycatView mycatView) {
                MycatViewDataNodeMapping mycatViewDataNodeMapping = mycatView.getMycatViewDataNodeMapping();
                SqlNode sqlTemplate = mycatView.getSQLTemplate(forUpdate);
                rel.dataNodeMapping = new ShardingTableDataNodeMapping(mycatViewDataNodeMapping, sqlTemplate);
                rel.columnInfo = new CopyMycatRowMetaData(new CalciteRowMetaData(mycatView.getRowType().getFieldList()));
            }
        });

        ClassDeclaration classDeclaration = mycatEnumerableRelImplementor.implementHybridRoot(relNode, EnumerableRel.Prefer.ARRAY);
        String code = Expressions.toString(classDeclaration.memberDeclarations, "\n", false);
        if (log.isDebugEnabled()) {
            log.debug("----------------------------------------code----------------------------------------");
            log.debug(code);
        }
        CodeContext codeContext = new CodeContext(classDeclaration.name, code);

        CodeExecuterContext executerContext = CodeExecuterContext.of(relContext, varContext, codeContext);
        return executerContext;
    }
//
//    @SneakyThrows
//    public Future<Void> runOnDrds(MycatDataContext dataContext,
//                                  DrdsSqlWithParams drdsSqlWithParams, Response response) {
//        DrdsSql drdsSql = this.preParse(statement);
//        Plan plan = getPlan(dataContext, drdsSql);
//        XaSqlConnection transactionSession = (XaSqlConnection) dataContext.getTransactionSession();
//        List<Object> params = drdsSqlWithParams.getParams();
//        PlanImplementor planImplementor = new ObservablePlanImplementorImpl(
//                transactionSession,
//                dataContext, params, response);
//        return impl(plan, planImplementor);
//    }

    @SneakyThrows
    public static Plan convertToExecuter(MycatRel mycatRel) {
        if (mycatRel instanceof MycatUpdateRel) {
            return new PlanImpl((MycatUpdateRel) mycatRel);
        } else if (mycatRel instanceof MycatInsertRel) {
            return new PlanImpl((MycatInsertRel) mycatRel);
        }
//        CodeExecuterContext codeExecuterContext = getCodeExecuterContext(Objects.requireNonNull(mycatRel), forUpdate);
//        return new PlanImpl(mycatRel, codeExecuterContext, forUpdate);
        return null;
    }


//    public Future<Void> runHbtOnDrds(MycatDataContext dataContext, String statement, Response response) {
//        XaSqlConnection transactionSession = (XaSqlConnection) dataContext.getTransactionSession();
//        List<Object> params = Collections.emptyList();
//        PlanImplementor planImplementor = new ObservablePlanImplementorImpl(
//                transactionSession,
//                dataContext, params, response);
//        Plan hbtPlan = getHbtPlan(statement);
//        return planImplementor.executeQuery(hbtPlan);
//    }
    private Future<Void> impl(Plan plan, PlanImplementor planImplementor) {
        switch (plan.getType()) {
            case PHYSICAL:
                return planImplementor.executeQuery(plan);
            case UPDATE:
                return planImplementor.executeUpdate( Objects.requireNonNull(plan));
            case INSERT:
                return planImplementor.executeInsert(Objects.requireNonNull(plan));
            default: {
                return VertxUtil.newFailPromise(new MycatException(MySQLErrorCode.ER_NOT_SUPPORTED_YET, "不支持的执行计划"));
            }
        }
    }

}
