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
package io.mycat.calcite.physical;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.*;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.RxBuiltInMethod;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * Values operator implemented in Mycat convention.
 */
public class MycatValues extends Values implements MycatRel {
    protected MycatValues(RelOptCluster cluster, RelDataType rowType,
                          ImmutableList<ImmutableList<RexLiteral>> tuples, RelTraitSet traitSet) {
        super(cluster, rowType, tuples, traitSet);
    }
    public static MycatValues create( RelOptCluster cluster, RelDataType rowType,
                                      ImmutableList<ImmutableList<RexLiteral>> tuples, RelTraitSet traitSet) {
        return new MycatValues(cluster, rowType, tuples, traitSet);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        return new MycatValues(getCluster(), rowType, tuples, traitSet);
    }


    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        return writer.name("MycatValues").item("values", tuples).ret();
    }


    @Override
    public Result implement(MycatEnumerableRelImplementor implementor, Prefer pref) {
/*
          return Linq4j.asEnumerable(
              new Object[][] {
                  new Object[] {1, 2},
                  new Object[] {3, 4}
              });
*/
        final JavaTypeFactory typeFactory =
                (JavaTypeFactory) getCluster().getTypeFactory();
        final BlockBuilder builder = new BlockBuilder();
        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getRowType(),
                        pref.preferCustom());
        final Type rowClass = physType.getJavaRowType();

        final List<Expression> expressions = new ArrayList<>();
        final List<RelDataTypeField> fields = rowType.getFieldList();
        for (List<RexLiteral> tuple : tuples) {
            final List<Expression> literals = new ArrayList<>();
            for (Pair<RelDataTypeField, RexLiteral> pair
                    : Pair.zip(fields, tuple)) {
                literals.add(
                        RexToLixTranslator.translateLiteral(
                                pair.right,
                                pair.left.getType(),
                                typeFactory,
                                RexImpTable.NullAs.NULL));
            }
            expressions.add(physType.record(literals));
        }
        builder.add(
                Expressions.return_(
                        null,
                        Expressions.call(
                                BuiltInMethod.AS_ENUMERABLE.method,
                                Expressions.newArrayInit(
                                        Primitive.box(rowClass), expressions))));
        return implementor.result(physType, builder.toBlock());
    }

//    @Override
//    public Result implementStream(StreamMycatEnumerableRelImplementor implementor, Prefer pref) {
//     /*
//          return Linq4j.asEnumerable(
//              new Object[][] {
//                  new Object[] {1, 2},
//                  new Object[] {3, 4}
//              });
//*/
//        final JavaTypeFactory typeFactory =
//                (JavaTypeFactory) getCluster().getTypeFactory();
//        final BlockBuilder builder = new BlockBuilder();
//        final PhysType physType =
//                PhysTypeImpl.of(
//                        implementor.getTypeFactory(),
//                        getRowType(),
//                        pref.preferCustom());
//        final Type rowClass = physType.getJavaRowType();
//
//        final List<Expression> expressions = new ArrayList<>();
//        final List<RelDataTypeField> fields = rowType.getFieldList();
//        for (List<RexLiteral> tuple : tuples) {
//            final List<Expression> literals = new ArrayList<>();
//            for (Pair<RelDataTypeField, RexLiteral> pair
//                    : Pair.zip(fields, tuple)) {
//                literals.add(
//                        RexToLixTranslator.translateLiteral(
//                                pair.right,
//                                pair.left.getType(),
//                                typeFactory,
//                                RexImpTable.NullAs.NULL));
//            }
//            expressions.add(physType.record(literals));
//        }
//        builder.add(
//                Expressions.return_(
//                        null,
//                        Expressions.call(
//                                RxBuiltInMethod.AS_OBSERVABLE.method,
//                                Expressions.newArrayInit(
//                                        Primitive.box(rowClass), expressions))));
//        return implementor.result(physType, builder.toBlock());
//    }

    @Override
    public boolean isSupportStream() {
        return false;
    }
}