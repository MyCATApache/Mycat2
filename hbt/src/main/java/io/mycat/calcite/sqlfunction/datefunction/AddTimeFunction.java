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

import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.mycat.MycatBuiltInMethodImpl;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.Temporal;
import java.util.List;

public  class AddTimeFunction extends SqlFunction {
    public static  final AddTimeFunction INSTANCE =new AddTimeFunction();

    public AddTimeFunction() {
      super("ADDTIME", SqlKind.OTHER_FUNCTION,
              ReturnTypes.VARCHAR_2000_NULLABLE
              , InferTypes.FIRST_KNOWN, OperandTypes.VARIADIC, SqlFunctionCategory.STRING);
    }
    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
      return super.checkOperandTypes(callBinding, throwOnFailure);
    }
    //SqlParserUtil
    //DateTimeUtils
    //SqlLiteral
    public static String addTime(String time, String tmp) {
      boolean sub  = false;
      return addTime(time, tmp, sub);
    }

    public static String addTime(String time, String tmp, boolean sub) {
      if (time == null || tmp == null) {
        return null;
      }
      Duration duration = MycatBuiltInMethodImpl.timeStringToTimeDuration(tmp);
      Temporal temporal;
      if (time.contains(":") && !time.contains("-")) {//time
        Duration duration1 = MycatBuiltInMethodImpl.timeStringToTimeDuration(time);
        duration1 =!sub?  duration1.plus(duration):duration1.minus(duration);
        long days1 = duration1.toDays();
        if (days1 == 0){
          long hours = java.util.concurrent.TimeUnit.SECONDS.toHours(duration1.getSeconds());
          int SECONDS_PER_HOUR = 60*60;
          int SECONDS_PER_MINUTE = 60;
          int minutes = (int) (( duration1.getSeconds()  % SECONDS_PER_HOUR) / SECONDS_PER_MINUTE);
          int secs = (int) ( duration1.getSeconds() % SECONDS_PER_MINUTE);
          int nano = duration1.getNano();
          //01:00:00.999999
          return String.format("%02d:%02d:%02d.%09d",hours, minutes, secs, nano);
        }else {
          long hours = java.util.concurrent.TimeUnit.SECONDS.toHours(duration1.getSeconds());
          int SECONDS_PER_HOUR = 60*60;
          int SECONDS_PER_MINUTE = 60;
          int minutes = (int) (( duration1.getSeconds()  % SECONDS_PER_HOUR) / SECONDS_PER_MINUTE);
          int secs = (int) ( duration1.getSeconds() % SECONDS_PER_MINUTE);
          int nano = duration1.getNano();
          return String.format("%02d:%02d:%02d:%02d.%09d", days1, hours, minutes, secs, nano);
        }
      }
      temporal = MycatBuiltInMethodImpl.timestampStringToTimestamp(time);

      Temporal res = !sub?addTime(temporal, duration):subTime(temporal,duration);
      if (res instanceof LocalDateTime) {
        LocalDateTime res1 = (LocalDateTime) res;
        return res1.toLocalDate().toString() + " " + res1.toLocalTime().toString();
      }
      if (res instanceof LocalTime) {
        LocalTime res1 = (LocalTime) res;
        return res1.toString();
      }
      return res.toString();
    }

    private static Temporal addTime(Temporal temporal, Duration duration) {
      if (temporal == null || duration == null) {
        return null;
      }
      Temporal plus = temporal.plus(duration);
      return plus;
    }
    private static Temporal subTime(Temporal temporal, Duration duration) {
      if (temporal == null || duration == null) {
        return null;
      }
      Temporal plus = temporal.minus(duration);
      return plus;
    }


    public RexImpTable.RexCallImplementor getRexCallImplementor(){
      return  new RexImpTable.MycatAbstractRexCallImplementor(NullPolicy.ANY, false) {

        @Override
        public String getVariableName() {
          return "ADDTIME";
        }

        @Override
        public Expression implementSafe(RexToLixTranslator translator, RexCall call, List<Expression> argValueList) {
          Expression one = argValueList.get(0);
          Expression second = argValueList.get(1);
          Class firstClass = (Class) one.getType();
          Class secondClass =(Class) second.getType();
          Method addtime = Types.lookupMethod(AddTimeFunction.class, "addTime", firstClass, secondClass);
          return Expressions.call(addtime,one,second);
        }
      };
    }
  }