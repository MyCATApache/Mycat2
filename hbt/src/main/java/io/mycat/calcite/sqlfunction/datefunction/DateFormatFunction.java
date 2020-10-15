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
import org.jetbrains.annotations.Nullable;

import java.time.DayOfWeek;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.format.TextStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.time.temporal.Temporal;
import java.time.temporal.WeekFields;
import java.util.Locale;


public class DateFormatFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(DateFormatFunction.class,
            "dateFormat");
    public static DateFormatFunction INSTANCE = new DateFormatFunction();


    public DateFormatFunction() {
        super("dateFormat", scalarFunction);
    }

    //SqlParserUtil
    //DateTimeUtils
    //SqlLiteral
    public static String dateFormat(String dateText,
                                    String format,
                                    @Parameter(name = "locale", optional = true) String localeText) {
        if (dateText == null || format == null) {
            return null;
        }
        Locale locale = Locale.US;
        if (localeText != null) {
            locale = Locale.forLanguageTag(localeText);
        }
        return dateFormat(dateText, format, locale);
    }

    @Nullable
    public static String dateFormat(String dateText, String format, Locale locale) {
        Temporal temporal = MycatBuiltInMethodImpl.timestampStringToUnixTimestamp(dateText);
        return dateFormat(format, locale, temporal);
    }

    @Nullable
    public static String dateFormat(String format, Locale locale, Temporal temporal) {
        DateTimeFormatterBuilder dateTimeFormatterBuilder = new DateTimeFormatterBuilder();
        int length = format.length();
        for (int i = 0; i < length; i++) {
            int next = i + 1;
            if (format.charAt(i) == '%' && next != length) {
                char c = format.charAt(next);
                if (isOption(c)) {
                    i+=1;
                    switch (c) {
                        case 'a': {
                            dateTimeFormatterBuilder.appendText(ChronoField.DAY_OF_WEEK, TextStyle.SHORT);
                            break;
                        }
                        case 'b': {
                            dateTimeFormatterBuilder.appendText(ChronoField.MONTH_OF_YEAR, TextStyle.SHORT);
                            break;
                        }
                        case 'c': {
                            dateTimeFormatterBuilder.appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NORMAL);
                            break;
                        }
                        case 'D': {

                            int day = temporal.get(ChronoField.DAY_OF_MONTH);
                            String text = String.format("%01d", temporal.get(ChronoField.DAY_OF_MONTH));
                            dateTimeFormatterBuilder.appendLiteral(text);
                            String order;
                            if (day >= 10 && day <= 19)
                                order = ("th");
                            else {
                                int tmp = (int) (day % 10);
                                switch (tmp) {
                                    case 1:
                                        order = ("st");
                                        break;
                                    case 2:
                                        order = ("nd");
                                        break;
                                    case 3:
                                        order = ("rd");
                                        break;
                                    default:
                                        order = ("th");
                                        break;
                                }
                            }
                            dateTimeFormatterBuilder.appendLiteral(order);
                            break;
                        }
                        case 'd': {
                            dateTimeFormatterBuilder.appendValue(ChronoField.DAY_OF_MONTH, 2);
                            break;
                        }
                        case 'e': {
                            dateTimeFormatterBuilder.appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NORMAL);
                            break;
                        }
                        case 'f': {
                            dateTimeFormatterBuilder.appendValue(ChronoField.INSTANT_SECONDS, 6);
                            break;
                        }
                        case 'H': {
                            dateTimeFormatterBuilder.appendValue(ChronoField.HOUR_OF_DAY, 2);
                            break;
                        }
                        case 'h':
                        case 'I': {
                            dateTimeFormatterBuilder.appendValue(ChronoField.CLOCK_HOUR_OF_AMPM, 2);
                            break;
                        }
                        case 'i': {
                            dateTimeFormatterBuilder.appendValue(ChronoField.MINUTE_OF_HOUR, 2);
                            break;
                        }
                        case 'j': {
                            dateTimeFormatterBuilder.appendValue(ChronoField.DAY_OF_YEAR, 3);
                            break;
                        }
                        case 'k': {
                            dateTimeFormatterBuilder.appendValue(ChronoField.HOUR_OF_DAY, 1,2,SignStyle.NORMAL);
                            break;
                        }
                        case 'l': {
                            dateTimeFormatterBuilder.appendText(ChronoField.CLOCK_HOUR_OF_AMPM);
                            break;
                        }
                        case 'M': {
                            dateTimeFormatterBuilder.appendText(ChronoField.MONTH_OF_YEAR, TextStyle.FULL);
                            break;
                        }
                        case 'm': {
                            dateTimeFormatterBuilder.appendValue(ChronoField.MONTH_OF_YEAR, 2);
                            break;
                        }
                        case 'p': {
                            dateTimeFormatterBuilder.appendText(ChronoField.AMPM_OF_DAY);
                            break;
                        }
                        case 'r': {
                            dateTimeFormatterBuilder
                                    .appendValue(ChronoField.CLOCK_HOUR_OF_AMPM)
                                    .appendLiteral(':')
                                    .appendValue(ChronoField.MINUTE_OF_HOUR)
                                    .appendLiteral(':')
                                    .appendValue(ChronoField.SECOND_OF_MINUTE,2,2,SignStyle.NORMAL)
                                    .appendLiteral(' ')
                                    .appendText(ChronoField.AMPM_OF_DAY);
                            break;
                        }
                        case 'S':
                        case 's': {
                            dateTimeFormatterBuilder.appendValue(ChronoField.SECOND_OF_MINUTE, 2);
                            break;
                        }
                        case 'T': {
                            dateTimeFormatterBuilder
                                    .appendValue(ChronoField.HOUR_OF_DAY)
                                    .appendLiteral(':')
                                    .appendValue(ChronoField.MINUTE_OF_HOUR)
                                    .appendLiteral(':')
                                    .appendValue(ChronoField.SECOND_OF_MINUTE,2,2,SignStyle.NORMAL);
                            break;
                        }
                        case 'U': {
                            //todo check
                            dateTimeFormatterBuilder
                                    .appendValue(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
                            break;
                        }
                        case 'u': {
                            //todo check
                            dateTimeFormatterBuilder
                                    .appendValue(ChronoField.ALIGNED_WEEK_OF_YEAR);
                            break;
                        }
                        case 'V': {
                            dateTimeFormatterBuilder
                                    .appendLiteral( String.format("%01d",temporal.get(WeekFields.ISO.weekOfWeekBasedYear())-1));
                            break;
                        }
                        case 'v': {
                            dateTimeFormatterBuilder
                                    .appendLiteral( String.format("%01d",temporal.get(WeekFields.SUNDAY_START.weekOfWeekBasedYear())-1));
                            break;
                        }
                        case 'W': {
                            DayOfWeek dayOfWeek = null;
                            if (temporal instanceof LocalDateTime){
                                 dayOfWeek = ((LocalDateTime) temporal).getDayOfWeek();
                            }
                            if (temporal instanceof LocalDate){
                                dayOfWeek = ((LocalDate) temporal).getDayOfWeek();
                            }
                            dateTimeFormatterBuilder
                                    .appendLiteral(dayOfWeek.getDisplayName(TextStyle.FULL,locale));
                            break;
                        }
                        case 'w': {
                            dateTimeFormatterBuilder
                                    .appendValue(ChronoField.DAY_OF_WEEK, 1);
                            break;
                        }
                        case 'X': {
                            //todo check
                            dateTimeFormatterBuilder
                                    .appendValue(WeekFields.ISO.weekBasedYear(), 4, 4, SignStyle.NORMAL);
                            break;
                        }
                        case 'x': {
                            //todo check
                            dateTimeFormatterBuilder
                                    .appendValue(WeekFields.SUNDAY_START.weekBasedYear(), 4, 4, SignStyle.NORMAL);
                            break;

                        }
                        case 'Y': {
                            dateTimeFormatterBuilder
                                    .appendValue(ChronoField.YEAR, 4, 4, SignStyle.NORMAL);
                            break;
                        }
                        case 'y': {
                            int i1 = temporal.get(ChronoField.YEAR);
                            String s = String.valueOf(i1);
                            if (s.length()>2){
                                s= s.substring(s.length()-2);
                            }
                            dateTimeFormatterBuilder
                                    .appendLiteral(s);
                            break;
                        }
                        case '#': {
                            for (int j = next; j < length; j++) {
                                if (Character.isDigit(format.charAt(j))) {
                                    continue;
                                } else {
                                    i = j;
                                }
                            }
                            break;
                        }
                        case '.': {
                            for (int j = next; j < length; j++) {
                                if (isPunctuation(format.charAt(j))) {
                                    continue;
                                } else {
                                    i = j;
                                }
                            }
                            break;
                        }
                        case '@': {
                            for (int j = next; j < length; j++) {
                                if (Character.isAlphabetic(format.charAt(j))) {
                                    continue;
                                } else {
                                    i = j;
                                }
                            }
                            break;
                        }
                        case '%': {
                            dateTimeFormatterBuilder
                                    .appendLiteral('%');
                            break;
                        }
                        default: {
                           throw new UnsupportedOperationException();
                        }

                    }
                } else {
                    dateTimeFormatterBuilder.appendLiteral(c);
                }
            } else {
                dateTimeFormatterBuilder.appendLiteral(format.charAt(i));
            }
        }
        DateTimeFormatter dateTimeFormatter = dateTimeFormatterBuilder.toFormatter(locale);

        if (temporal instanceof LocalDate) {
            return ((LocalDate) temporal).format(dateTimeFormatter);
        }
        if (temporal instanceof LocalDateTime) {
            return ((LocalDateTime) temporal).format(dateTimeFormatter);
        }
        return null;
    }

    public static boolean isOption(char c) {
        switch (c) {
            case 'a':
            case 'b':
            case 'c':
            case 'D':
            case 'd':
            case 'e':
            case 'f':
            case 'H':
            case 'h':
            case 'I':
            case 'i':
            case 'j':
            case 'k':
            case 'l':
            case 'M':
            case 'm':
            case 'p':
            case 'r':
            case 'S':
            case 's':
            case 'T':
            case 'U':
            case 'u':
            case 'V':
            case 'v':
            case 'W':
            case 'w':
            case 'X':
            case 'x':
            case 'Y':
            case 'y':
            case '#':
            case '.':
            case '@':
            case '%':
                return true;
            default: {
                return false;
            }
        }
    }

    static boolean isPunctuation(char ch) {
        if (isCjkPunc(ch)) return true;
        if (isEnPunc(ch)) return true;

        if (0x2018 <= ch && ch <= 0x201F) return true;
        if (ch == 0xFF01 || ch == 0xFF02) return true;
        if (ch == 0xFF07 || ch == 0xFF0C) return true;
        if (ch == 0xFF1A || ch == 0xFF1B) return true;
        if (ch == 0xFF1F || ch == 0xFF61) return true;
        if (ch == 0xFF0E) return true;
        if (ch == 0xFF65) return true;

        return false;
    }

    static boolean isEnPunc(char ch) {
        if (0x21 <= ch && ch <= 0x22) return true;
        if (ch == 0x27 || ch == 0x2C) return true;
        if (ch == 0x2E || ch == 0x3A) return true;
        if (ch == 0x3B || ch == 0x3F) return true;

        return false;
    }

    static boolean isCjkPunc(char ch) {
        if (0x3001 <= ch && ch <= 0x3003) return true;
        if (0x301D <= ch && ch <= 0x301F) return true;

        return false;
    }

    private static Temporal addTime(Temporal temporal, Duration duration) {
        if (temporal == null || duration == null) {
            return null;
        }
        Temporal plus = temporal.plus(duration);
        return plus;
    }

    @Override
    public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
        return super.deriveType(validator, scope, call);
    }
}

