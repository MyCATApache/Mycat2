package io.mycat.upondb;

import com.alibaba.fastsql.sql.ast.expr.SQLDateExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLDateTimeExpr;
import com.google.common.collect.ImmutableSet;
import io.mycat.plug.sequence.SequenceGenerator;
import io.mycat.util.MySQLFunction;
import io.mycat.util.SQLContext;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Supplier;

public class MysqlFunctions {
    public static final MySQLFunction next_value_for = new MySQLFunction() {
        @Override
        public Set<String> getFunctionNames() {
            return ImmutableSet.of( "next_value_for");
        }

        @Override
        public int getArgumentSize() {
            return 1;
        }

        @Override
        public Object eval(SQLContext context, Object[] args) {
            String name = args[0].toString().replaceAll("MYCATSEQ_", "");
            Supplier<String> sequence = SequenceGenerator.INSTANCE.getSequence(name);
            String s = sequence.get();
            return s;
        }
    };

    public static final MySQLFunction last_insert_id = new MySQLFunction() {
        @Override
        public Set<String> getFunctionNames() {
            return ImmutableSet.of("last_insert_id");
        }

        @Override
        public int getArgumentSize() {
            return 0;
        }

        @Override
        public Object eval(SQLContext context, Object[] args) {
            return context.lastInsertId();
        }
    };

    //SELECT current_user() mysql workbench
    public static final MySQLFunction current_user = new MySQLFunction() {
        @Override
        public Set<String> getFunctionNames() {
            return ImmutableSet.of("current_user");
        }

        @Override
        public int getArgumentSize() {
            return 0;
        }

        @Override
        public Object eval(SQLContext context, Object[] args) {
            return context.getSQLVariantRef("current_user");
        }
    };

    public static final MySQLFunction CURRENT_DATE = new MySQLFunction() {
        @Override
        public Set<String> getFunctionNames() {
            return ImmutableSet.of("CURRENT_DATE", "CURDATE", "curdate");
        }

        @Override
        public int getArgumentSize() {
            return 0;
        }

        @Override
        public Object eval(SQLContext context, Object[] args) {
            return LocalDate.now().toString();
        }
    };
    public static final MySQLFunction NOW = new MySQLFunction() {
        @Override
        public Set<String> getFunctionNames() {
            return ImmutableSet.of("NOW");
        }

        @Override
        public int getArgumentSize() {
            return 0;
        }

        @Override
        public Object eval(SQLContext context, Object[] args) {
            return  LocalDateTime.now().toString();
        }
    };
}