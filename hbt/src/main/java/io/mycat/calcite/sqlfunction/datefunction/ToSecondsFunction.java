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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

//unsupport SELECT TO_SECONDS(130513);
public class ToSecondsFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(ToSecondsFunction.class,
            "toSeconds");
    public static ToSecondsFunction INSTANCE = new ToSecondsFunction();

    public ToSecondsFunction() {
        super("TO_SECONDS",
                scalarFunction
        );
    }

    public static Long toSeconds(LocalDateTime date) {
        if (date==null) {
            return null;
        }
        //@todo
        LocalDateTime startDate = LocalDate.of(0, 1, 1).atStartOfDay();
        return (ChronoUnit.SECONDS.between(startDate, date));
    }
}
