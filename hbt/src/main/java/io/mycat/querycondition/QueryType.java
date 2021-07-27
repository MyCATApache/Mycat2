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
package io.mycat.querycondition;

/**
 * Query type of a push down condition in InnoDB data source.
 */
public enum QueryType {
    /**
     * Primary key point query.
     */
    PK_POINT_QUERY(0, 0.1),
    /** Secondary key point query. */
    /**
     * Primary key range query with lower and upper bound.
     */
    PK_RANGE_QUERY(2, 0.5),

    /**
     * Scanning table fully with primary key.
     */
    PK_FULL_SCAN(4, 1),
    ;

    private final int priority;
    private final double factor;

    public static QueryType getPointQuery() {
        return PK_POINT_QUERY;
    }

    public static QueryType getRangeQuery() {
        return PK_RANGE_QUERY;
    }

    QueryType(int priority, double factor) {
        this.priority = priority;
        this.factor = factor;
    }

    public int priority() {
        return priority;
    }

    public double factor() {
        return factor;
    }
}
