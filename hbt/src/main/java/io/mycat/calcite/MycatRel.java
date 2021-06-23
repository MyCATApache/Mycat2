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
package io.mycat.calcite;

import com.google.common.collect.ImmutableList;
import io.reactivex.rxjava3.core.Observable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.RxBuiltInMethod;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.List;

/**
 * Relational expression that uses JDBC calling convention.
 */
public interface MycatRel extends RelNode, EnumerableRel, Serializable {

    ExplainWriter explain(ExplainWriter writer);

    public static ExplainWriter explainJoin(Join join, String name, ExplainWriter writer) {
        writer.name(name);
        List<String> fieldList = join.getRowType().getFieldNames();
        writer.item("columns", String.join(",", fieldList));
        SqlImplementor.Context context = explainRex(MycatSqlDialect.DEFAULT, fieldList);
        SqlNode sqlNode = context.toSql(null, join.getCondition());
        writer.item("condition", sqlNode);
        writer.into();
        ((MycatRel) join.getLeft()).explain(writer);
        ((MycatRel) join.getRight()).explain(writer);
        return writer.ret();
    }

    public static SqlImplementor.Context explainRex(SqlDialect dialect, List<String> fieldList) {
        return new SqlImplementor.Context(dialect, fieldList.size()) {
            @Override
            public SqlNode field(int ordinal) {
                String fieldName = fieldList.get(ordinal);
                return new SqlIdentifier(ImmutableList.of(fieldName), SqlImplementor.POS);
            }

            @Override
            public SqlImplementor implementor() {
                return null;
            }
        };
    }

    @Override
    default Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        if (implementor instanceof MycatEnumerableRelImplementor) {
            return implement((MycatEnumerableRelImplementor) implementor, pref);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    default Result implement(MycatEnumerableRelImplementor implementor, Prefer pref) {
        throw new UnsupportedOperationException();
    }

//    default Result implementHybrid(MycatRel input, StreamMycatEnumerableRelImplementor implementor, Prefer pref) {
//        boolean thisSupportStream = isSupportStream();
//        MycatRel subMycatRel = (MycatRel) input;
//        if (thisSupportStream) {
//            boolean subSupportStream = subMycatRel.isSupportStream();
//            if (subSupportStream) {
//                return input.implementStream(implementor, pref);
//            } else {
//                return input.implement(implementor, pref);
//            }
//        } else {
//            if (input.isSupportStream()) {
//                MycatMatierial mycatMatierial = MycatMatierial.create(getCluster(),
//                        getCluster().traitSetOf(MycatConvention.INSTANCE), subMycatRel);
//                return mycatMatierial.implementStream(implementor, pref);
//            } else {
//                return input.implement(implementor, pref);
//            }
//        }
//    }

   default Result implementStream(StreamMycatEnumerableRelImplementor implementor, Prefer pref) {
        throw new UnsupportedOperationException();
    }

    default boolean isSupportStream() {
        return false;
    }

    @NotNull
    public default Expression toEnumerate(Expression input) {
        if (!isSupportStream()){
            Type type = input.getType();
            if (!(type instanceof Enumerable)) {
                input = Expressions.call(RxBuiltInMethod.TO_ENUMERABLE.method, input);
            }
            return input;
        }else {
            return input;
        }
    }
    public default Expression toObservable(Expression input) {
            Type type = input.getType();
            if ((type instanceof Enumerable)) {
              return Expressions.call(RxBuiltInMethod.TO_OBSERVABLE.method, input);
            }else {
                return input;
            }
    }
    public default Expression toObservableCache(Expression input) {
            return Expressions.call(RxBuiltInMethod.TO_OBSERVABLE_CACHE.method, input);

    }
}
