package io.mycat;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.MySQLDatasource;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.buffer.DefaultReactorBufferPool;
import io.mycat.command.CommandDispatcher;
import io.mycat.config.*;
import io.mycat.proxy.MySQLDatasourcePool;
import io.mycat.proxy.reactor.*;
import io.mycat.proxy.session.*;
import io.mycat.replica.ReplicaSelectorManager;
import lombok.Getter;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.sql.JDBCType;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

@Getter
public class NativeMycatServer implements MycatServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(NativeMycatServer.class);

    private final ConcurrentHashMap<String, MySQLDatasourcePool> datasourceMap = new ConcurrentHashMap<>();

    private final MycatServerConfig serverConfig;

    private ReactorThreadManager reactorManager;

    private MycatWorkerProcessor mycatWorkerProcessor;

    private DatasourceConfigProvider datasourceConfigProvider;

    private Authenticator authenticator;


    @SneakyThrows
    public NativeMycatServer(MycatServerConfig serverConfig) {
        this.serverConfig = serverConfig;

    }

    @SneakyThrows
    public void start() {
        this.authenticator = new ProxyAuthenticator();
        this.datasourceConfigProvider = new ProxyDatasourceConfigProvider();

        this.mycatWorkerProcessor = MetaClusterCurrent.wrapper(MycatWorkerProcessor.class);
        startProxy(this.serverConfig.getServer());
    }


    private void startProxy(io.mycat.config.ServerConfig serverConfig) throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, java.lang.reflect.InvocationTargetException, IOException, InterruptedException {
        String handlerConstructorText = "io.mycat.commands.DefaultCommandHandler";
        DefaultReactorBufferPool defaultReactorBufferPool = new DefaultReactorBufferPool(Optional
                .ofNullable(serverConfig).map(i -> i.getBufferPool()).map(i -> i.getArgs()).orElse(BufferPoolConfig.defaultValue()));
        Constructor<?> handlerConstructor = getConstructor(handlerConstructorText);
        int reactorNumber = Optional.ofNullable(serverConfig).map(i -> i.getReactorNumber()).orElse(1);
        List<MycatReactorThread> list = new ArrayList<>(reactorNumber);
        for (int i = 0; i < reactorNumber; i++) {
            Function<MycatSession, CommandDispatcher> function = session -> {
                try {
                    CommandDispatcher commandDispatcher = (CommandDispatcher) handlerConstructor.newInstance();
                    commandDispatcher.initRuntime(session);
                    return commandDispatcher;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            };
            MycatReactorThread thread = new MycatReactorThread(defaultReactorBufferPool, new MycatSessionManager(function));
            thread.start();
            list.add(thread);
        }

        this.reactorManager = new ReactorThreadManager(list);
        NIOAcceptor acceptor = new NIOAcceptor(reactorManager);
        acceptor.startServerChannel(serverConfig.getIp(), serverConfig.getPort());
        LOGGER.info("mycat starts successful");
    }

    public MySQLDatasourcePool getDatasource(String name) {
        MySQLDatasourcePool datasource = datasourceMap.get(name);

        if (datasource != null) {
            return datasource;
        }
        DatasourceConfig datasourceConfig = Objects.requireNonNull(datasourceConfigProvider.get()).get(name);
        if (datasourceConfig != null && "mysql".equalsIgnoreCase(datasourceConfig.getDbType())) {
            return datasourceMap.computeIfAbsent(name, s -> {
                MySQLDatasourcePool mySQLDatasource = new MySQLDatasourcePool(name,datasourceConfigProvider,this);
                return mySQLDatasource;
            });
        }
        return null;
    }


    private static Constructor<?> getConstructor(String clazz) throws ClassNotFoundException, NoSuchMethodException {
        Class<?> bufferPoolClass = Class.forName(clazz);
        return bufferPoolClass.getDeclaredConstructor();
    }

    private static void runExtra(List<String> objectMap) {
        for (String clazz : Optional.ofNullable(objectMap)
                .orElse(Collections.emptyList())) {
            try {
                Class<?> aClass = Class.forName(clazz);
                Constructor<?> declaredConstructor = aClass.getDeclaredConstructors()[0];
                Runnable o = (Runnable) declaredConstructor.newInstance();
                o.run();
            } catch (Throwable e) {
                LOGGER.error("can not run:{}", clazz, e);
            }
        }
    }

    @Override
    public RowBaseIterator showNativeDataSources() {
        MycatRouterConfig mycatRouterConfig = MetaClusterCurrent.wrapper(MycatRouterConfig.class);
        Map<String, DatasourceConfig> datasourceConfigMap = mycatRouterConfig.getDatasources().stream().collect(Collectors.toMap(k -> k.getName(), v -> v));

        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();

        resultSetBuilder.addColumnInfo("NAME", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("USERNAME", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("PASSWORD", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("MAX_CON", JDBCType.BIGINT);
        resultSetBuilder.addColumnInfo("MIN_CON", JDBCType.BIGINT);
        resultSetBuilder.addColumnInfo("EXIST_CON", JDBCType.BIGINT);
        resultSetBuilder.addColumnInfo("USE_CON", JDBCType.BIGINT);
        resultSetBuilder.addColumnInfo("MAX_RETRY_COUNT", JDBCType.BIGINT);
        resultSetBuilder.addColumnInfo("MAX_CONNECT_TIMEOUT", JDBCType.BIGINT);
        resultSetBuilder.addColumnInfo("DB_TYPE", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("URL", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("WEIGHT", JDBCType.VARCHAR);

        resultSetBuilder.addColumnInfo("INIT_SQL", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("INIT_SQL_GET_CONNECTION", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("INSTANCE_TYPE", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("IDLE_TIMEOUT", JDBCType.BIGINT);
        resultSetBuilder.addColumnInfo("DRIVER", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("TYPE", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("IS_MYSQL", JDBCType.VARCHAR);

        Map<String, Integer> map =getDatasourceMap().values().stream().collect(Collectors.toMap(k->k.getName(),v->v.getAllSessions().size()));
        for (MySQLDatasource value : getDatasourceMap().values()) {
            String NAME = value.getName();
            Optional<DatasourceConfig> e = Optional.ofNullable(datasourceConfigMap.get(NAME));

            String IP = value.getIp();
            int PORT = value.getPort();
            String USERNAME = value.getUsername();
            String PASSWORD = value.getPassword();
            int MAX_CON = value.getSessionLimitCount();
            int MIN_CON = value.getSessionMinCount();
            long USED_CON = map.getOrDefault(NAME, -1);
            int EXIST_CON = value.getConnectionCounter();
            int MAX_RETRY_COUNT = value.gerMaxRetry();
            long MAX_CONNECT_TIMEOUT = value.getMaxConnectTimeout();
            String DB_TYPE = "mysql";
            String URL = null;
            int WEIGHT = e.map(i -> i.getWeight()).orElse(-1);
            String INIT_SQL = value.getInitSqlForProxy();
            boolean INIT_SQL_GET_CONNECTION = false;
            ReplicaSelectorManager selectorRuntime = MetaClusterCurrent.wrapper(ReplicaSelectorManager.class);
            String INSTANCE_TYPE = Optional.ofNullable(selectorRuntime.getPhysicsInstanceByName(NAME)).map(i -> i.getType().name()).orElse(e.map(i -> i.getInstanceType()).orElse(null));
            long IDLE_TIMEOUT = value.getIdleTimeout();

            String DRIVER = "native";//保留属性
            String TYPE = e.map(i -> i.getType()).orElse(null);
            boolean IS_MYSQL = true;

            resultSetBuilder.addObjectRowPayload(Arrays.asList(NAME, IP, PORT, USERNAME, PASSWORD, MAX_CON, MIN_CON, EXIST_CON, USED_CON,
                    MAX_RETRY_COUNT, MAX_CONNECT_TIMEOUT, DB_TYPE, URL, WEIGHT, INIT_SQL, INIT_SQL_GET_CONNECTION, INSTANCE_TYPE,
                    IDLE_TIMEOUT, DRIVER, TYPE, IS_MYSQL));
        }
        return resultSetBuilder.build();
    }

    @Override
    public RowBaseIterator showConnections() {
        Objects.requireNonNull(reactorManager);
        List<MycatSession> sessions = reactorManager.getList().stream()
                .flatMap(i -> i.getFrontManager().getAllSessions().stream())
                .collect(Collectors.toList());

        ResultSetBuilder builder = ResultSetBuilder.create();

        builder.addColumnInfo("ID", JDBCType.BIGINT);
        builder.addColumnInfo("USER_NAME", JDBCType.VARCHAR);
        builder.addColumnInfo("HOST", JDBCType.VARCHAR);
        builder.addColumnInfo("SCHEMA", JDBCType.VARCHAR);
        builder.addColumnInfo("AFFECTED_ROWS", JDBCType.BIGINT);
        builder.addColumnInfo("AUTOCOMMIT", JDBCType.VARCHAR);
        builder.addColumnInfo("IN_TRANSACTION", JDBCType.VARCHAR);
        builder.addColumnInfo("CHARSET", JDBCType.VARCHAR);
        builder.addColumnInfo("CHARSET_INDEX", JDBCType.BIGINT);
        builder.addColumnInfo("OPEN", JDBCType.VARCHAR);
        builder.addColumnInfo("SERVER_CAPABILITIES", JDBCType.BIGINT);
        builder.addColumnInfo("ISOLATION", JDBCType.VARCHAR);
        builder.addColumnInfo("LAST_ERROR_CODE", JDBCType.BIGINT);
        builder.addColumnInfo("LAST_INSERT_ID", JDBCType.BIGINT);
        builder.addColumnInfo("LAST_MESSAGE", JDBCType.VARCHAR);
        builder.addColumnInfo("PROCESS_STATE", JDBCType.VARCHAR);
        builder.addColumnInfo("WARNING_COUNT", JDBCType.BIGINT);
        builder.addColumnInfo("MYSQL_SESSION_ID", JDBCType.BIGINT);
        builder.addColumnInfo("TRANSACTION_TYPE", JDBCType.VARCHAR);
        builder.addColumnInfo("TRANSCATION_SNAPSHOT", JDBCType.VARCHAR);
        builder.addColumnInfo("CANCEL_FLAG", JDBCType.VARCHAR);

        for (MycatSession session : sessions) {
            long ID = session.sessionId();
            MycatUser user = session.getUser();
            String USER_NAME = user.getUserName();
            String HOST = user.getHost();
            String SCHEMA = session.getSchema();
            long AFFECTED_ROWS = session.getAffectedRows();
            boolean AUTOCOMMIT = session.isAutocommit();
            boolean IN_TRANSACTION = session.isInTransaction();
            String CHARSET = Optional.ofNullable(session.charset()).map(i -> i.displayName()).orElse("");
            int CHARSET_INDEX = session.charsetIndex();
            boolean OPEN = session.checkOpen();
            int SERVER_CAPABILITIES = session.getServerCapabilities();
            String ISOLATION = session.getIsolation().getText();
            int LAST_ERROR_CODE = session.getLastErrorCode();
            long LAST_INSERT_ID = session.getLastInsertId();
            String LAST_MESSAGE = session.getLastMessage();
            String PROCESS_STATE = session.getProcessState().name();

            int WARNING_COUNT = session.getWarningCount();
            Long MYSQL_SESSION_ID = Optional.ofNullable(session.getMySQLSession()).map(i -> i.sessionId()).orElse(null);


            MycatDataContext dataContext = session.getDataContext();
            String TRANSACTION_TYPE = Optional.ofNullable(dataContext.transactionType()).map(i -> i.getName()).orElse("");

            TransactionSession transactionSession = dataContext.getTransactionSession();
            String TRANSCATION_SMAPSHOT = transactionSession.snapshot().toString("|");
            boolean CANCEL_FLAG = dataContext.getCancelFlag().get();
            builder.addObjectRowPayload(Arrays.asList(
                    ID,
                    USER_NAME,
                    HOST,
                    SCHEMA,
                    AFFECTED_ROWS,
                    AUTOCOMMIT,
                    IN_TRANSACTION,
                    CHARSET,
                    CHARSET_INDEX,
                    OPEN,
                    SERVER_CAPABILITIES,
                    ISOLATION,
                    LAST_ERROR_CODE,
                    LAST_INSERT_ID,
                    LAST_MESSAGE,
                    PROCESS_STATE,
                    WARNING_COUNT,
                    MYSQL_SESSION_ID,
                    TRANSACTION_TYPE,
                    TRANSCATION_SMAPSHOT,
                    CANCEL_FLAG
            ));
        }
        return builder.build();
    }

    @Override
    public RowBaseIterator showReactors() {
        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
        resultSetBuilder.addColumnInfo("THREAD_NAME", JDBCType.VARCHAR)
                .addColumnInfo("THREAD_ID", JDBCType.BIGINT)
                .addColumnInfo("CUR_SESSION_ID", JDBCType.BIGINT)
                .addColumnInfo("PREPARE_STOP", JDBCType.VARCHAR)
                .addColumnInfo("BUFFER_POOL_SNAPSHOT", JDBCType.VARCHAR)
                .addColumnInfo("LAST_ACTIVE_TIME", JDBCType.TIMESTAMP);
        for (MycatReactorThread mycatReactorThread : getReactorManager().getList()) {
            String THREAD_NAME = mycatReactorThread.getName();
            long THREAD_ID = mycatReactorThread.getId();
            Long CUR_SESSION_ID = Optional.ofNullable(mycatReactorThread.getCurSession()).map(i -> i.sessionId()).orElse(null);
            boolean PREPARE_STOP = mycatReactorThread.isPrepareStop();
            String BUFFER_POOL_SNAPSHOT = Optional.ofNullable(mycatReactorThread.getBufPool()).map(i -> i.snapshot().toString("|")).orElse("");
            LocalDateTime LAST_ACTIVE_TIME = new Timestamp(mycatReactorThread.getLastActiveTime()).toLocalDateTime();
            resultSetBuilder.addObjectRowPayload(Arrays.asList(
                    THREAD_NAME,
                    THREAD_ID,
                    CUR_SESSION_ID,
                    PREPARE_STOP,
                    BUFFER_POOL_SNAPSHOT,
                    LAST_ACTIVE_TIME
            ));
        }
        return resultSetBuilder.build();
    }

    @Override
    public RowBaseIterator showBufferUsage(long sessionId) {
        MycatSession curSession = null;
        for (MycatReactorThread mycatReactorThread : getReactorManager().getList()) {
            SessionManager.FrontSessionManager<MycatSession> frontManager = mycatReactorThread.getFrontManager();
            for (MycatSession session : frontManager.getAllSessions()) {
                if (sessionId == session.sessionId()) {
                    curSession = session;
                    break;
                }
            }
        }
        ResultSetBuilder builder = ResultSetBuilder.create();
        builder.addColumnInfo("bufferUsage", JDBCType.BIGINT);
        if (curSession != null) {
            builder.addObjectRowPayload(Arrays.asList(curSession.writeBufferPool().trace()));
        }
        return builder.build();
    }

    @Override
    public RowBaseIterator showNativeBackends() {
        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
        resultSetBuilder.addColumnInfo("SESSION_ID", JDBCType.BIGINT)
                .addColumnInfo("THREAD_NAME", JDBCType.VARCHAR)
                .addColumnInfo("THREAD_ID", JDBCType.BIGINT)
                .addColumnInfo("DS_NAME", JDBCType.VARCHAR)
                .addColumnInfo("LAST_MESSAGE", JDBCType.VARCHAR)
                .addColumnInfo("MYCAT_SESSION_ID", JDBCType.BIGINT)
                .addColumnInfo("IS_IDLE", JDBCType.VARCHAR)
                .addColumnInfo("SELECT_LIMIT", JDBCType.BIGINT)
                .addColumnInfo("IS_AUTOCOMMIT", JDBCType.VARCHAR)
                .addColumnInfo("IS_RESPONSE_FINISHED", JDBCType.VARCHAR)
                .addColumnInfo("RESPONSE_TYPE", JDBCType.VARCHAR)
                .addColumnInfo("IS_IN_TRANSACTION", JDBCType.VARCHAR)
                .addColumnInfo("IS_REQUEST_SUCCESS", JDBCType.VARCHAR)
                .addColumnInfo("IS_READ_ONLY", JDBCType.VARCHAR);
        for (MycatReactorThread i : getReactorManager().getList()) {
            List<MySQLClientSession> sqlClientSessions = datasourceMap.values().stream().flatMap(s -> s.getAllSessions().stream()).collect(Collectors.toList());
            for (MySQLClientSession session : sqlClientSessions) {

                long SESSION_ID = session.sessionId();
                String THREAD_NAME = i.getName();
                long THREAD_ID = i.getId();
                String DS_NAME = session.getDatasource().getName();
                String LAST_MESSAGE = session.getLastMessage();
                Long MYCAT_SESSION_ID = Optional.ofNullable(session.getMycat()).map(m -> m.sessionId()).orElse(null);
                boolean IS_IDLE = session.isIdle();

                long SELECT_LIMIT = session.getSelectLimit();
                boolean IS_AUTOCOMMIT = session.isAutomCommit() == MySQLAutoCommit.ON;
                boolean IS_RESPONSE_FINISHED = session.isResponseFinished();
                String RESPONSE_TYPE = Optional.ofNullable(session.getResponseType()).map(j -> j.name()).orElse(null);
                boolean IS_IN_TRANSACTION = session.isMonopolizedByTransaction();
                boolean IS_REQUEST_SUCCESS = session.isRequestSuccess();
                boolean IS_READ_ONLY = session.isReadOnly();

                resultSetBuilder.addObjectRowPayload(Arrays.asList(
                        SESSION_ID,
                        THREAD_NAME,
                        THREAD_ID,
                        DS_NAME,
                        LAST_MESSAGE,
                        MYCAT_SESSION_ID,
                        IS_IDLE,
                        SELECT_LIMIT,
                        IS_AUTOCOMMIT,
                        IS_RESPONSE_FINISHED,
                        RESPONSE_TYPE,
                        IS_IN_TRANSACTION,
                        IS_REQUEST_SUCCESS,
                        IS_READ_ONLY
                ));
            }

        }
        return resultSetBuilder.build();
    }
}