/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.time.LocalDateTime;
import java.util.Locale;

public class GetFormatFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(GetFormatFunction.class,
            "getFormat");
    public static GetFormatFunction INSTANCE = new GetFormatFunction();

    public GetFormatFunction() {
        super("GET_FORMAT", scalarFunction);
    }

    public static String getFormat(String date, String format) {
        String agr0 = date.toUpperCase();
        String agr1 = format.toUpperCase();
        switch (agr0) {
            case "DATE":
                switch (agr1) {
                    case "USA":
                        return "%m.%d.%Y";
                    case "JIS":
                    case "ISO":
                        return "%Y-%m-%d";
                    case "EUR":
                        return "%d.%m.%Y";
                    case "INTERNAL":
                        return "%Y%m%d";
                }
                break;
            case "DATETIME":
                switch (agr1) {
                    case "USA":
                    case "EUR":
                        return "%Y-%m-%d %H.%i.%s";
                    case "JIS":
                    case "ISO":
                        return "%Y-%m-%d %H:%i:%s";
                    case "INTERNAL":
                        return "%Y%m%d%H%i%s";
                }
                break;
            case "TIME":
                switch (agr1) {
                    case "USA":
                        return "%h:%i:%s %p";
                    case "JIS":
                    case "ISO":
                        return "%H:%i:%s";
                    case "EUR":
                        return "%H.%i.%s";
                    case "INTERNAL":
                        return "%H%i%s";
                }
                break;
        }
        return null;
    }
}

