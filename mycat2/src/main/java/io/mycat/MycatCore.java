/**
 * Copyright (C) <2019>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat;

import io.mycat.buffer.BufferPool;
import io.mycat.buffer.HeapBufferPool;
import io.mycat.calcite.MetadataManager;
import io.mycat.client.ClientRuntime;
import io.mycat.command.CommandDispatcher;
import io.mycat.config.*;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.plug.PlugRuntime;
import io.mycat.proxy.buffer.ProxyBufferPoolMonitor;
import io.mycat.proxy.reactor.*;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.MycatSessionManager;
import io.mycat.beans.MySQLDatasource;
import io.mycat.replica.ReplicaSelectorRuntime;
import lombok.SneakyThrows;

import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * @author cjw
 **/
public enum MycatCore {
    INSTANCE;
    private ConfigProvider config;
    private static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(MycatCore.class);
    private ConcurrentHashMap<String, MySQLDatasource> datasourceMap = new ConcurrentHashMap<>();

    @SneakyThrows
    public void init(ConfigProvider config) {
        this.config = config;

        MycatConfig mycatConfig = config.currentConfig();

        PlugRuntime.INSTCANE.load(mycatConfig);
        JdbcRuntime.INSTANCE.load(mycatConfig);
        ClientRuntime.INSTANCE.load(mycatConfig);
        ReplicaSelectorRuntime.INSTANCE.load(mycatConfig);

        MetadataManager.INSTANCE.load(mycatConfig);
        ServerConfig serverConfig = mycatConfig.getServer();

        String bufferPoolText = Optional.ofNullable(mycatConfig.getServer()).map(i -> i.getBufferPool()).map(i -> i.getPoolName()).orElse(HeapBufferPool.class.getName());
        String handlerConstructorText = Optional.ofNullable(mycatConfig.getServer()).map(i -> i.getHandlerName()).orElse(DefaultCommandHandler.class.getName());

        Constructor<?> bufferPoolConstructor = getConstructor(bufferPoolText);
        Constructor<?> handlerConstructor = getConstructor(handlerConstructorText);

        int reactorNumber = serverConfig.getReactorNumber();
        List<MycatReactorThread> list = new ArrayList<>(reactorNumber);
        for (int i = 0; i < reactorNumber; i++) {
            BufferPool bufferPool = (BufferPool) bufferPoolConstructor.newInstance();
            bufferPool.init(mycatConfig.getServer().getBufferPool().getArgs());
            Function<MycatSession, CommandDispatcher> function = session -> {
                try {
                    CommandDispatcher commandDispatcher = (CommandDispatcher) handlerConstructor.newInstance();
                    commandDispatcher.initRuntime(session);
                    return commandDispatcher;
                }catch (Exception e){
                    throw new RuntimeException(e);
                }
            };
            MycatReactorThread thread = new MycatReactorThread(new ProxyBufferPoolMonitor(bufferPool), new MycatSessionManager(function));
            thread.start();
            list.add(thread);
        }

        final ReactorThreadManager reactorManager = new ReactorThreadManager(list);
        TimerConfig timer = mycatConfig.getDatasource().getTimer();
        ScheduleUtil.getTimer().scheduleAtFixedRate(() -> {
            for (MycatReactorThread thread : reactorManager.getList()) {
                thread.addNIOJob(new NIOJob() {
                    @Override
                    public void run(ReactorEnvThread reactor) throws Exception {
                        thread.getMySQLSessionManager().idleConnectCheck();
                    }

                    @Override
                    public void stop(ReactorEnvThread reactor, Exception reason) {

                    }

                    @Override
                    public String message() {
                        return "idleConnectCheck";
                    }
                });
            }
        },timer.getInitialDelay(),timer.getPeriod(), TimeUnit.valueOf(timer.getTimeUnit()));
        NIOAcceptor acceptor = new NIOAcceptor(reactorManager);
        acceptor.startServerChannel(serverConfig.getIp(), serverConfig.getPort());
    }

    public static void main(String[] args) throws Exception {
        ConfigProvider bootConfig = RootHelper.INSTCANE.bootConfig(MycatCore.class);
        MycatCore.INSTANCE.init(bootConfig);
    }

    public MySQLDatasource getDatasource(String name) {
        MySQLDatasource datasource2 = datasourceMap.get(name);
        if (datasource2 != null) {
            return datasource2;
        }
        List<DatasourceRootConfig.DatasourceConfig> datasources = config.currentConfig().getDatasource().getDatasources();
        for (DatasourceRootConfig.DatasourceConfig datasourceConfig : datasources) {
            if (name.equals(datasourceConfig.getName())) {
                return datasourceMap.computeIfAbsent(name, s -> new MySQLDatasource(datasourceConfig) {
                });
            }
        }
        throw new IllegalArgumentException();
    }

    public void removeDatasource(String name) {
        datasourceMap.remove(name);
    }

    private static Constructor<?> getConstructor(String clazz) throws ClassNotFoundException, NoSuchMethodException {
        Class<?> bufferPoolClass = Class.forName(clazz);
        return bufferPoolClass.getDeclaredConstructor();
    }
}
