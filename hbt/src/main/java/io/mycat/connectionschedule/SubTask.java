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
package io.mycat.connectionschedule;


import io.vertx.core.Promise;
import io.vertx.sqlclient.SqlConnection;
import lombok.Getter;

import java.util.Comparator;
import java.util.function.ToIntFunction;

@Getter
public class SubTask implements Comparable<SubTask> {
        final int order;
        final String target;
        final int count;
        final Promise<SqlConnection> promise;
        final long deadline;
        static final Comparator<SubTask> subTaskComparator = Comparator.comparingInt((ToIntFunction<SubTask>) value -> value.order)
                .thenComparing((Comparator.comparingInt((ToIntFunction<SubTask>) value -> value.count).reversed()));

        public SubTask(int order, int count, String target, Promise<SqlConnection> promise, long deadline) {
            this.order = order;
            this.target = target;
            this.count = count;
            this.promise = promise;
            this.deadline = deadline;
        }

        @Override
        public int compareTo( SubTask o) {
            return subTaskComparator.compare(this, o);
        }
    }