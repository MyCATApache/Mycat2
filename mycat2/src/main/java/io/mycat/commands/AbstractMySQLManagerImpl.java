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

import cn.mycat.vertx.xa.MySQLManager;
import io.mycat.ScheduleUtil;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;

import java.util.concurrent.TimeUnit;

public abstract class AbstractMySQLManagerImpl implements MySQLManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMySQLManagerImpl.class);

    @Override
    public void setTimer(long delay, Runnable handler) {
        ScheduleUtil.getTimer().schedule(() -> {
            try {
                handler.run();
            } catch (Exception e) {
                LOGGER.error(e);
            }
        }, delay, TimeUnit.MILLISECONDS);
    }
}
