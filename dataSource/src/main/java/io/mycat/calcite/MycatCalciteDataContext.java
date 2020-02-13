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
package io.mycat.calcite;

import com.google.common.collect.ImmutableMap;
import io.mycat.QueryBackendTask;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.TransactionSessionUtil;
import io.mycat.replica.ReplicaSelectorRuntime;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.util.Holder;

import java.util.ArrayList;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Junwen Chen
 **/
public  class MycatCalciteDataContext implements DataContext, AutoCloseable {
    private final SchemaPlus rootSchema;
    private final Map<String, Object> variables;
    private final AtomicBoolean cancelFlag = new AtomicBoolean(false);
    private final ArrayList<DefaultConnection> dsConnections = new ArrayList<>(1);

    public MycatCalciteDataContext(SchemaPlus rootSchema, Map<String, Object> variables) {
        this.rootSchema = rootSchema;
        this.variables = variables;
    }

    public MycatCalciteDataContext(SchemaPlus rootSchema) {
        this(rootSchema, TimeZone.getDefault(), "sa", Locale.getDefault());
    }

    public MycatCalciteDataContext(SchemaPlus rootSchema, TimeZone timeZone, String user, Locale locale) {
        final Holder<Long> timeHolder = Holder.of(System.currentTimeMillis());
        final long time = timeHolder.get();
        final long localOffset = timeZone.getOffset(time);
        final long currentOffset = localOffset;
        final String systemUser = System.getProperty("user.name");

        final Holder<Object[]> streamHolder =
                Holder.of(new Object[]{System.in, System.out, System.err});

        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        builder.put(Variable.UTC_TIMESTAMP.camelName, time)
                .put(Variable.CURRENT_TIMESTAMP.camelName, time + currentOffset)
                .put(Variable.LOCAL_TIMESTAMP.camelName, time + localOffset)
                .put(Variable.TIME_ZONE.camelName, timeZone)
                .put(Variable.USER.camelName, user)
                .put(Variable.SYSTEM_USER.camelName, systemUser)
                .put(Variable.LOCALE.camelName, locale)
                .put(Variable.STDIN.camelName, streamHolder.get()[0])
                .put(Variable.STDOUT.camelName, streamHolder.get()[1])
                .put(Variable.STDERR.camelName, streamHolder.get()[2])
                .put(Variable.CANCEL_FLAG.camelName, cancelFlag);

        this.rootSchema = rootSchema;
        this.variables = builder.build();
    }

    public SchemaPlus getRootSchema() {
        return rootSchema;
    }

    public JavaTypeFactory getTypeFactory() {
        return MycatCalciteContext.INSTANCE.TypeFactory;
    }

    public QueryProvider getQueryProvider() {
        return null;
    }

    public Object get(String name) {
        if (variables == null) {
            return null;
        }
        return variables.get(name);
    }

    @Override
    public void close() throws Exception {
        dsConnections.forEach(i->i.close());
        dsConnections.clear();
    }

    public DefaultConnection getTarget(QueryBackendTask endTableInfo) {
        String datasourceName= ReplicaSelectorRuntime.INSTANCE.getDatasourceNameByReplicaName(endTableInfo.getTargetName(),false,null);
        DefaultConnection session = TransactionSessionUtil.currentTransactionSession().getDisposableConnection(datasourceName);
        dsConnections.add(session);
        return session;
    }
}