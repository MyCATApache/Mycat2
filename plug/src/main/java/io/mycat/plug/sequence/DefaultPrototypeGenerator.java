/**
 * Copyright (C) <2021>  <chen junwen>
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
package io.mycat.plug.sequence;

import io.mycat.*;
import io.vertx.core.Vertx;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.MessageFormat;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class DefaultPrototypeGenerator implements Supplier<Number> {
    protected static final Logger LOGGER = LoggerFactory.getLogger(DefaultPrototypeGenerator.class);
    private final String targetName;
    private final String nextSql;
    private final String setValueSql;
    private final ScheduledFuture<?> scheduledFuture;

    private volatile AtomicLong count = new AtomicLong();
    private volatile long step;
    private volatile long limit;

    private int inv = 30;

    private Value value = null;

    public DefaultPrototypeGenerator(Map<String, Object> config) {
        this.targetName = Objects.toString(config.getOrDefault("targetName", "prototype"));
        this.nextSql = Objects.toString(config.getOrDefault("nextSql", "select nextval('{0}')"));
        this.setValueSql = Objects.toString((config.getOrDefault("setValueSql", "")));
        this.scheduledFuture = ScheduleUtil.getTimer().scheduleAtFixedRate(this::fetch,
                0, (int) (inv * 0.8),
                TimeUnit.MILLISECONDS);
    }

    @Override
    @SneakyThrows
    public Number get() {
        long andAccumulate = count.getAndUpdate(operand -> operand + 1);
        if (andAccumulate < limit) {
            return andAccumulate;
        }
        Value andUpdate = null;
        synchronized (this) {
            andUpdate = this.value;
            this.value = null;
            if (andUpdate != null) {
                count.set(andUpdate.count);
                step = andUpdate.step;
                limit = count.get() + step;
            }
        }
        if (andUpdate != null) {
            fetch();
        } else {
            Thread.sleep(inv / 2);
        }
        return get();
    }

    public void fetch() {
        IOExecutor vertx = MetaClusterCurrent.wrapper(IOExecutor.class);
        vertx.executeBlocking(promise -> {
            try{
                if (this.value == null) {
                    Value number = getNumber();
                    synchronized (this) {
                        if (this.value == null) {
                            this.value = number;
                        }
                    }
                }
            }catch (Throwable throwable){
                LOGGER.error("",throwable);
            }finally {
                promise.tryComplete();
            }
        });
    }

    @NotNull
    @SneakyThrows
    public synchronized Value getNumber() {
        ConnectionManager connectionManager = MetaClusterCurrent.wrapper(ConnectionManager.class);
        try (MycatConnection prototype = connectionManager.getConnection(this.targetName)) {
            Connection connection = prototype.unwrap(Connection.class);
            PreparedStatement preparedStatement = connection.prepareStatement(this.nextSql);
            ResultSet resultSet = preparedStatement.executeQuery();
            resultSet.next();
            long count = resultSet.getLong(1);
            long step = resultSet.getLong(2);
            return new Value(count, step);
        }
    }

    @SneakyThrows
    public void setStart(String schema, String table, long value) {
        ConnectionManager connectionManager = MetaClusterCurrent.wrapper(ConnectionManager.class);
        try (MycatConnection prototype = connectionManager.getConnection(this.targetName)) {
            prototype.executeUpdate(MessageFormat.format(setValueSql, schema, table, value), false);
        }
    }

    @Getter
    @AllArgsConstructor
    static class Value {
        private final long count;
        private final long step;
    }
}