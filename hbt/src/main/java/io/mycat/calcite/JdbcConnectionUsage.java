//package io.mycat.calcite;
//
//import cn.mycat.vertx.xa.MySQLManager;
//import cn.mycat.vertx.xa.XaSqlConnection;
//import com.google.common.collect.ImmutableMultimap;
//import io.mycat.MetaClusterCurrent;
//import io.mycat.MycatDataContext;
//import io.mycat.calcite.executor.MycatPreparedStatementUtil;
//import io.mycat.calcite.logical.MycatView;
//import io.mycat.calcite.plan.ObservablePlanImplementorImpl;
//import io.mycat.calcite.resultset.CalciteRowMetaData;
//import io.mycat.calcite.table.MycatTransientSQLTableScan;
//import io.mycat.connectionschedule.CloseFuture;
//import io.mycat.connectionschedule.SchedulePolicy;
//import io.mycat.connectionschedule.SubTask;
//import io.mycat.util.VertxUtil;
//import io.mycat.vertx.VertxExecuter;
//import io.reactivex.rxjava3.annotations.NonNull;
//import io.reactivex.rxjava3.core.Observable;
//import io.reactivex.rxjava3.core.ObservableEmitter;
//import io.vertx.core.*;
//import io.vertx.sqlclient.SqlConnection;
//import org.apache.calcite.rel.RelNode;
//import org.apache.calcite.sql.util.SqlString;
//import org.jetbrains.annotations.NotNull;
//import org.jetbrains.annotations.Nullable;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.*;
//import java.util.concurrent.PriorityBlockingQueue;
//import java.util.concurrent.atomic.AtomicBoolean;
//import java.util.function.ToIntFunction;
//import java.util.stream.Collectors;
//
//import static io.mycat.calcite.JdbcConnectionUsage.Scheduler.addTask;
//
//public class JdbcConnectionUsage {
//    private final static Logger LOGGER = LoggerFactory.getLogger(JdbcConnectionUsage.class);
//
//
//
//}