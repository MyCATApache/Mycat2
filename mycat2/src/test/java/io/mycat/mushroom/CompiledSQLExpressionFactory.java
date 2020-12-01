package io.mycat.mushroom;

import org.apache.calcite.MycatContext;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class CompiledSQLExpressionFactory {
    final MycatContext context;
    final Supplier<Object[]> rowGetter;


    public CompiledSQLExpressionFactory(MycatContext context, Supplier<Object[]> rowGetter) {
        this.context = context;
        this.rowGetter = rowGetter;
    }

    public CompiledSQLExpression createAccessCurrentRowExpression(
            String name,
            CompiledSQLExpression compileExpression
    ) {
        return new AccessFieldExpression<Object[]>(name, compileExpression) {
        };
    }

    public CompiledSQLExpression createRexCorrelVariable(int id, String name) {
        return null;
    }

    public CompiledSQLExpression createEqualsExpression(CompiledSQLExpression compileExpression, CompiledSQLExpression compileExpression1) {
        return new EqualsExpression() {
            CompiledSQLExpression[] compileExpressions = new CompiledSQLExpression[]{compileExpression, compileExpression1};

            @Override
            public Object readByIndex(Object o, int index) {
                return compileExpressions[index].eval(o);
            }
        };
    }

    public CompiledSQLExpression createNotEqualsExpression(CompiledSQLExpression compileExpression, CompiledSQLExpression compileExpression1) {
        return new NotEqualsExpression() {
            CompiledSQLExpression[] compileExpressions = new CompiledSQLExpression[]{compileExpression, compileExpression1};

            @Override
            public Object readByIndex(Object o, int index) {
                return compileExpressions[index].eval(o);
            }
        };
    }

    public CompiledSQLExpression createGreatThanExpression(CompiledSQLExpression compileExpression, CompiledSQLExpression compileExpression1) {
        return new GreatThanExpression() {
            CompiledSQLExpression[] compileExpressions = new CompiledSQLExpression[]{compileExpression, compileExpression1};

            @Override
            public Object readByIndex(Object o, int index) {
                return compileExpressions[index].eval(o);
            }
        };
    }

    public CompiledSQLExpression createGreatThanOrEqualExpression(CompiledSQLExpression compileExpression, CompiledSQLExpression compileExpression1) {
        return new GreatThanOrEqualExpression() {
            CompiledSQLExpression[] compileExpressions = new CompiledSQLExpression[]{compileExpression, compileExpression1};

            @Override
            public Object readByIndex(Object o, int index) {
                return compileExpressions[index].eval(o);
            }
        };
    }

    public CompiledSQLExpression createLessThanExpression(CompiledSQLExpression compileExpression, CompiledSQLExpression compileExpression1) {
        return new LessThanExpression() {
            CompiledSQLExpression[] compileExpressions = new CompiledSQLExpression[]{compileExpression, compileExpression1};

            @Override
            public Object readByIndex(Object o, int index) {
                return compileExpressions[index].eval(o);
            }
        };
    }

    public CompiledSQLExpression createLessThanOrEqualExpression(CompiledSQLExpression compileExpression, CompiledSQLExpression compileExpression1) {
        return new LessThanOrEqualExpression() {
            CompiledSQLExpression[] compileExpressions = new CompiledSQLExpression[]{compileExpression, compileExpression1};

            @Override
            public Object readByIndex(Object o, int index) {
                return compileExpressions[index].eval(o);
            }
        };
    }

    public CompiledSQLExpression createPlusExpression(CompiledSQLExpression compileExpression, CompiledSQLExpression compileExpression1) {
        return new PlusExpression<Object[]>(){
            CompiledSQLExpression[] compileExpressions = new CompiledSQLExpression[]{compileExpression, compileExpression1};

            @Override
            public Object readByIndex(Object[] objects, int index) {
                return compileExpressions[index].eval(objects);
            }
        };
    }

    public CompiledSQLExpression createModExpression(CompiledSQLExpression compileExpression, CompiledSQLExpression compileExpression1) {
        return new ModExpression<Object[]>(){
            CompiledSQLExpression[] compileExpressions = new CompiledSQLExpression[]{compileExpression, compileExpression1};

            @Override
            public Object readByIndex(Object[] objects, int index) {
                return compileExpressions[index].eval(objects);
            }
        };
    }

    public CompiledSQLExpression createMinusExpression(CompiledSQLExpression compileExpression, CompiledSQLExpression compileExpression1) {
        return new MinusExpression<Object[]>(){
            CompiledSQLExpression[] compileExpressions = new CompiledSQLExpression[]{compileExpression, compileExpression1};

            @Override
            public Object readByIndex(Object[] objects, int index) {
                return compileExpressions[index].eval(objects);
            }
        };
    }

    public CompiledSQLExpression createMultiplyExpression(CompiledSQLExpression compileExpression, CompiledSQLExpression compileExpression1) {
        return new MinusExpression<Object[]>(){
            CompiledSQLExpression[] compileExpressions = new CompiledSQLExpression[]{compileExpression, compileExpression1};

            @Override
            public Object readByIndex(Object[] objects, int index) {
                return compileExpressions[index].eval(objects);
            }
        };
    }

    static CompiledSQLExpression createSignedExpression(CompiledSQLExpression compileExpression) {
        return new SignedExpression(){
            @Override
            public Object readInput(Object o) {
                return compileExpression.eval(o);
            }
        };
    }

    static CompiledSQLExpression createDivideExpression(CompiledSQLExpression compileExpression, CompiledSQLExpression compileExpression1) {
        return new MinusExpression<Object[]>(){
            CompiledSQLExpression[] compileExpressions = new CompiledSQLExpression[]{compileExpression, compileExpression1};

            @Override
            public Object readByIndex(Object[] objects, int index) {
                return compileExpressions[index].eval(objects);
            }
        };
    }

    static CompiledSQLExpression createLikeExpression(CompiledSQLExpression expression, CompiledSQLExpression compileExpression, CompiledSQLExpression compileExpression1) {
        return new LikeExpression<Object[]>(){
            CompiledSQLExpression[] compileExpressions = new CompiledSQLExpression[]{expression,compileExpression, compileExpression1};

            @Override
            public Object readByIndex(Object[] objects, int index) {
                return compileExpressions[index].eval(objects);
            }
        };
    }

    static CompiledSQLExpression createAndExpression(
            CompiledSQLExpression compileExpression, CompiledSQLExpression compileExpression1) {
        return new AndExpression<Object[]>() {
            CompiledSQLExpression[] compileExpressions = new CompiledSQLExpression[]{compileExpression, compileExpression1};

            @Override
            public Object readByIndex(Object[] objects, int index) {
                return compileExpressions[index].eval(objects);
            }
        };
    }

    static CompiledSQLExpression createOrExpression(CompiledSQLExpression compileExpression, CompiledSQLExpression compileExpression1) {
        return new OrExpression<Object[]>() {
            @Override
            public Object readInput(Object[] objects) {
                return compileExpression.eval(objects);
            }
        };
    }

    static CompiledSQLExpression createNotExpression(CompiledSQLExpression compileExpression) {
        return new NotExpression<Object[]>() {
            @Override
            public Object readInput(Object[] objects) {
                return compileExpression.eval(objects);
            }
        };
    }

    static CompiledSQLExpression createIsNotNullExpression(CompiledSQLExpression compileExpression) {
        return new IsNotNullExpression<Object[]>() {
            @Override
            public Object readInput(Object[] objects) {
                return compileExpression.eval(objects);
            }
        };
    }

    static CompiledSQLExpression createIsNotTrueExpression(CompiledSQLExpression compileExpression) {
        return new IsNotTrueExpression<Object[]>() {
            @Override
            public Object readInput(Object[] objects) {
                return compileExpression.eval(objects);
            }
        };
    }

    static CompiledSQLExpression createIsNullExpression(CompiledSQLExpression compileExpression) {
        return new IsNullExpression<Object[]>() {
            @Override
            public Object readInput(Object[] o) {
                return compileExpression.eval(o);
            }
        };
    }

    static CompiledSQLExpression createCaseExpression(
            List<Map.Entry<CompiledSQLExpression, CompiledSQLExpression>> cases,
            CompiledSQLExpression elseExp) {
        return new CaseExpression<Object[]>(cases, elseExp) {

        };
    }


    public CompiledSQLExpression createRexDynamicParam(int index, RelDataType type) {
        return new RexDynamicParamExpression<Object[]>(index, type, context) {

        };
    }

    public CompiledSQLExpression createConstantNullExpression() {
        return new ConstantNullExpression() {

        };
    }

    public CompiledSQLExpression createConstantExpression(Object value3) {
        return new ConstantExpression(value3) {
        };
    }

    public CompiledSQLExpression createAccessCurrentRowExpression(int index, RelDataType type) {
        return new AccessCurrentRowExpression(index, context, type) {
        };
    }


    public CompiledSQLExpression createCastExpression(CompiledSQLExpression compileExpression, RelDataType type) {
        return new CastExpression(type) {
            @Override
            public Object readInput(Object o) {
                return compileExpression.eval(o);
            }
        };
    }
}
