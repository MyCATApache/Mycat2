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
package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.linq4j.function.Parameter;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Locale;

public class UnixTimestampFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(UnixTimestampFunction.class,
            "unixTimestamp");
    public static UnixTimestampFunction INSTANCE = new UnixTimestampFunction();

    public UnixTimestampFunction() {
        super("UNIX_TIMESTAMP", scalarFunction);
    }

    public static long unixTimestamp(@Parameter(name = "date", optional = true) LocalDateTime date) {
        if (date == null) {
            return System.currentTimeMillis() / 1000;
        } else {
            return Timestamp.valueOf(date).getTime() / 1000;
        }
    }
}

