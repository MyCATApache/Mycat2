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
package io.mycat.commands;

import io.mycat.newquery.NewMycatConnection;
import io.vertx.core.Future;
import io.vertx.sqlclient.SqlConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public  abstract class AbstractMycatDatasourcePool implements MycatDatasourcePool{
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatDatasourcePool.class);
    protected final String targetName;

    public AbstractMycatDatasourcePool(String targetName) {
        this.targetName = targetName;
    }

    public abstract Future<NewMycatConnection> getConnection();

    public abstract Integer getAvailableNumber();
    public abstract Integer getUsedNumber();
    public String getTargetName() {
        return targetName;
    }
}
