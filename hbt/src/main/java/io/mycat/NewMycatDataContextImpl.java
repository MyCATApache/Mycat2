/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mycat;

import io.mycat.calcite.CodeExecuterContext;
import io.vertx.core.Future;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.runtime.NewMycatDataContext;
import org.apache.calcite.schema.SchemaPlus;
import org.slf4j.LoggerFactory;

import java.util.IdentityHashMap;
import java.util.List;

public abstract class NewMycatDataContextImpl implements NewMycatDataContext {
    protected final MycatDataContext dataContext;
    protected final CodeExecuterContext codeExecuterContext;
    protected final List<Object> params;
    protected final boolean forUpdate;

    public NewMycatDataContextImpl(MycatDataContext dataContext,
                                   CodeExecuterContext context,
                                   List<Object> params,
                                   boolean forUpdate) {
        this.dataContext = dataContext;
        this.codeExecuterContext = context;
        this.params = params;
        this.forUpdate = forUpdate;
    }

    @Override
    public SchemaPlus getRootSchema() {
        return null;
    }

    @Override
    public JavaTypeFactory getTypeFactory() {
        return null;
    }

    @Override
    public QueryProvider getQueryProvider() {
        return null;
    }

    @Override
    public Object get(String name) {
        if (name.startsWith("?")) {
            int index = Integer.parseInt(name.substring(1));
            return params.get(index);
        }
        return codeExecuterContext.getVarContext().get(name);
    }

    public Object getSessionVariable(String name) {
        return dataContext.getVariable(false, name);
    }

    public Object getGlobalVariable(String name) {
        return dataContext.getVariable(true, name);
    }

    public String getDatabase() {
        return dataContext.getDefaultSchema();
    }

    public Long getLastInsertId() {
        return dataContext.getLastInsertId();
    }

    public Long getConnectionId() {
        return dataContext.getSessionId();
    }

    public Object getUserVariable(String name) {
        return null;
    }

    public String getCurrentUser() {
        MycatUser user = dataContext.getUser();
        Authenticator authenticator = MetaClusterCurrent.wrapper(Authenticator.class);
        return user.getUserName() + "@" + authenticator.getUserInfo(user.getUserName()).getIp();
    }

    public String getUser() {
        MycatUser user = dataContext.getUser();
        return user.getUserName() + "@" + user.getHost();
    }

    @Override
    public List<Object> getParams() {
        return params;
    }

    @Override
    public boolean isForUpdate() {
        return forUpdate;
    }

    @Override
    public Long getRowCount() {
        return dataContext.getAffectedRows();
    }
}
