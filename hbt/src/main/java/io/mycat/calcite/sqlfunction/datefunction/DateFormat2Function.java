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
import org.apache.calcite.mycat.MycatBuiltInMethodImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.format.TextStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.time.temporal.Temporal;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;


public class DateFormat2Function extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(DateFormat2Function.class,
            "dateFormat");
    public static DateFormat2Function INSTANCE = new DateFormat2Function();


    public DateFormat2Function() {
        super("DATE_FORMAT", scalarFunction);
    }

    //SqlParserUtil
    //DateTimeUtils
    //SqlLiteral
    public static String dateFormat(String dateText,
                                    String format) {
       return DateFormatFunction.dateFormat(dateText, format, Locale.getDefault().toLanguageTag());
    }
    @Override
    public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
        return super.deriveType(validator, scope, call);
    }
}

