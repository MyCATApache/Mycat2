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

import java.time.LocalDate;
import java.time.YearMonth;

public class LastDayFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(LastDayFunction.class,
            "lastDay");
    public static LastDayFunction INSTANCE = new LastDayFunction();

    public LastDayFunction() {
        super("LAST_DAY", scalarFunction);
    }


    public static LocalDate lastDay(LocalDate date) {
        if (date == null)return null;
        return    YearMonth.from(date).atEndOfMonth();
    }
//    public static LocalDate lastDay(Period date){
//        if (date == null){
//            return null;
//        }
//        if (date.isNegative()){
//            throw new UnsupportedOperationException();
//        }
//       return LocalDate.of(date.getMonths(),date.getMonths(),date.getDays());
//    }

}

