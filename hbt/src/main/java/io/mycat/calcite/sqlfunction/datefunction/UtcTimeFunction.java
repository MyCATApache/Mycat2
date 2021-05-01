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

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;

public class UtcTimeFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(UtcTimeFunction.class,
            "utcTime");
    public static UtcTimeFunction INSTANCE = new UtcTimeFunction();

    public UtcTimeFunction() {
        super("UTC_TIME", scalarFunction);
    }

    public static Duration utcTime(@Parameter(name = "precision", optional = true) Integer precision) {
        if (precision == null) {
            return Duration.ofSeconds(LocalTime.now().toSecondOfDay());
        }
        LocalTime now = LocalTime.now();
        int nano = now.getNano();
        String s = Integer.toString(nano);
        if (s.length() > precision) {
            s = s.substring(0, precision);
            nano = Integer.parseInt(s)*(int)Math.pow(10,9-precision);
        }
        return Duration.ofSeconds(now.toSecondOfDay(), now.getNano());
    }
}

