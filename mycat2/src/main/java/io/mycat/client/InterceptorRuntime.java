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

package io.mycat.client;

import com.google.common.collect.ImmutableMap;
import io.mycat.MycatConfig;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.booster.CacheConfig;
import io.mycat.commands.*;
import io.mycat.config.PatternRootConfig;
import io.mycat.matcher.Matcher;
import io.mycat.matcher.StringEqualsFactory;
import io.mycat.plug.command.MycatCommandLoader;
import io.mycat.util.Pair;
import io.mycat.util.StringUtil;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author Junwen Chen
 **/
public enum InterceptorRuntime {
    INSTANCE;
    private static final Logger LOGGER = LoggerFactory.getLogger(InterceptorRuntime.class);
    final Map<String, UserSpace> wapper = new ConcurrentHashMap<>();
    volatile MycatConfig mycatConfig;
    public static final String DISTRIBUTED_QUERY = "distributedQuery";
    public static final String EXECUTE_PLAN = "executePlan";


    public Interceptor login(String userName) {
        UserSpace userSpace = Objects.requireNonNull(wapper.get(userName));
        return new Interceptor(userSpace);
    }

    final static Map<String, Object> MYCAT_DB_COMMAND = (Map) ImmutableMap.builder().put("command", MycatdbCommand.INSTANCE.getName()).put("name", "defaultMycatdb").build();

    @SneakyThrows
    private synchronized void flash() {

        addCommand(BeginCommand.INSTANCE);
        addCommand(CommitCommand.INSTANCE);
        addCommand(DefErrorCommand.INSTANCE);
        addCommand(DistributedDeleteCommand.INSTANCE);
        addCommand(DistributedInsertCommand.INSTANCE);
        addCommand(DistributedQueryCommand.INSTANCE);
        addCommand(DistributedUpdateCommand.INSTANCE);
        addCommand(ExecuteCommand.INSTANCE);
        addCommand(ExplainPlanCommand.INSTANCE);
        addCommand(ExplainSqlCommand.INSTANCE);
        addCommand(MycatdbCommand.INSTANCE);
        addCommand(OffXACommand.INSTANCE);
        addCommand(OkCommand.INSTANCE);
        addCommand(OnXACommand.INSTANCE);
        addCommand(RollbackCommand.INSTANCE);
        addCommand(SelectAutocommitCommand.INSTANCE);
        addCommand(SelectLastInsertIdCommand.INSTANCE);
        addCommand(SelectTransactionReadOnlyCommand.INSTANCE);
        addCommand(SetAutoCommitOffCommand.INSTANCE);
        addCommand(SetAutoCommitOnCommand.INSTANCE);
        addCommand(SetTransactionIsolationCommand.INSTANCE);
        addCommand(UseStatementCommand.INSTANCE);
        addCommand(BoostMycatdbCommand.INSTANCE);

        //config
        this.wapper.clear();
        for (PatternRootConfig interceptor : Objects.requireNonNull(this.mycatConfig).getInterceptors()) {
            PatternRootConfig.UserConfig user = Objects.requireNonNull(interceptor.getUser());
            String username = Objects.requireNonNull(user.getUsername());
            String matcherClazz = interceptor.getMatcherClazz();
            Map<String, Object> defaultHanlder = interceptor.getDefaultHanlder();
            if (defaultHanlder == null) defaultHanlder = MYCAT_DB_COMMAND;
            List<Map<String, Object>> sqls = interceptor.getSqls();
            if (sqls == null) sqls = Collections.emptyList();
            if (matcherClazz == null) matcherClazz = StringEqualsFactory.class.getCanonicalName();
            Class<?> aClass = Class.forName(matcherClazz);
            Matcher.Factory factory = (Matcher.Factory) aClass.newInstance();
            List<Pair> allItems = sqls.stream().map(i -> Pair.of(i.get("sql"), i)).collect(Collectors.toList());

            List<CacheTask> cacheTasks = new ArrayList<>();
            for (Map<String, Object> sql : sqls) {
                String name = (String) sql.get("name");
                if (name == null) {
                    name = Objects.toString(sql);
                    sql.put("name", name);
                }
                String command = (String) sql.get("command");
                Type type = null;
                String cache;
                if (!DISTRIBUTED_QUERY.equalsIgnoreCase(command)) {
                    if (!EXECUTE_PLAN.equalsIgnoreCase(command)) {
                        continue;
                    } else {
                        type = Type.HBT;
                    }
                } else {
                    type = Type.SQL;
                }
                if (type != null && !StringUtil.isEmpty(cache = (String) sql.get("cache"))) {
                    String text = (String) sql.getOrDefault("explain", sql.get("sql"));
                    cacheTasks.add(new CacheTask(name, text, type, CacheConfig.create(cache)));
                } else {
                    continue;
                }
            }
            final Matcher apply = factory.create(allItems, Pair.of(null, defaultHanlder));
            TransactionType transactionType = TransactionType.parse(interceptor.getTransactionType());
            this.wapper.put(username, new UserSpace(username, transactionType, apply, cacheTasks));
        }
    }

    private void addCommand(MycatCommand instance) {
        MycatCommandLoader.INSTANCE.registerIfAbsent(instance.getName(), instance);
    }

    public synchronized void load(MycatConfig config) {
        if (this.mycatConfig == config) {
            return;
        } else {
            this.mycatConfig = config;
        }
        flash();
    }


}