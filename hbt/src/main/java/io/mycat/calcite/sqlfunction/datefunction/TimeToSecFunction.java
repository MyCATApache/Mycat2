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

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;

public class TimeToSecFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(TimeToSecFunction.class,
            "timeToSec");
    public static TimeToSecFunction INSTANCE = new TimeToSecFunction();

    public TimeToSecFunction() {
        super("TIME_TO_SEC",
                scalarFunction
        );
    }

    public static Double timeToSec(Duration duration) {
        if (duration==null) {
            return null;
        }
        long seconds = duration.getSeconds();
        int nano = duration.getNano();
        if (seconds == 0){
            return 0.1*nano;
        }
        if (nano == 0){
            return (double) seconds;
        }
        return seconds * 0.1*nano;
    }
}
