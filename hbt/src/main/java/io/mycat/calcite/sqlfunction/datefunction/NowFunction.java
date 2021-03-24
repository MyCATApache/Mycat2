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

import org.apache.calcite.linq4j.function.Parameter;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;

import java.time.LocalDateTime;
import java.time.ZoneId;

public class NowFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(NowFunction.class,
            "now");
    public static NowFunction INSTANCE = new NowFunction();

    public NowFunction() {
        super("NOW",
                scalarFunction
        );
    }

    public static LocalDateTime now(@Parameter(name = "precision") Integer precision) {
        if (precision == null) {
            return LocalDateTime.now(ZoneId.systemDefault()).now().withNano(0);
        }
        LocalDateTime now = LocalDateTime.now();
        int nano = now.getNano();
        //999,999,999

        int i1 =(int) Math.pow(10,(9 - precision));
        nano= nano/i1*i1;
        return now.withNano(nano);
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        super.unparse(writer, call, leftPrec, rightPrec);
    }
}
