package io.ordinate.engine;

import io.ordinate.engine.builder.*;
import io.ordinate.engine.function.aggregate.CountAggregateFunction;
import io.ordinate.engine.function.bind.IndexedParameterLinkFunction;
import io.ordinate.engine.function.bind.IntBindVariable;
import io.ordinate.engine.function.bind.VariableParameterFunction;
import io.ordinate.engine.physicalplan.OutputLinq4jPhysicalPlan;
import io.ordinate.engine.physicalplan.PhysicalPlan;
import io.ordinate.engine.record.RootContext;
import io.ordinate.engine.schema.ArrowTypes;
import io.ordinate.engine.schema.InnerType;
import io.reactivex.rxjava3.core.Observable;
import junit.framework.TestCase;
import org.apache.calcite.linq4j.JoinType;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ExecuteCompilerTest extends TestCase {
    RootContext rootContext =new RootContext();
    @Test
    public void testProject() {
        ExecuteCompiler relBuilder = new ExecuteCompiler();
        relBuilder.values(SchemaBuilder.ofArrowType(ArrowTypes.INT32_TYPE, ArrowTypes.STRING_TYPE).toArrow(), Arrays.asList(new Object[]{1, "2"}, new Object[]{3, "4"}));
        relBuilder.filter(relBuilder.makeLiteral(true));
        relBuilder.project(relBuilder.column(0));
        PhysicalPlan build = relBuilder.build();
        OutputLinq4jPhysicalPlan outputLinq4jPhysicalPlan = new OutputLinq4jPhysicalPlan(build);
        Observable<Object[]> execute = outputLinq4jPhysicalPlan.executeToObject(getRootContext());
        List<Object[]> objects = execute.toList().blockingGet();
        Assert.assertEquals("[[1], [3]]",print(objects));
    }
    @Test
    public void testFilter() {
        ExecuteCompiler relBuilder = new ExecuteCompiler();
        relBuilder.values(SchemaBuilder.ofArrowType(ArrowTypes.INT32_TYPE, ArrowTypes.STRING_TYPE).toArrow(), Arrays.asList(new Object[]{1, "2"}, new Object[]{3, "4"}));
        relBuilder.filter(relBuilder.call("=",relBuilder.column(0),relBuilder.makeLiteral(1)));
        relBuilder.project(relBuilder.column(0));
        PhysicalPlan build = relBuilder.build();
        OutputLinq4jPhysicalPlan outputLinq4jPhysicalPlan = new OutputLinq4jPhysicalPlan(build);
        Observable<Object[]> execute = outputLinq4jPhysicalPlan.executeToObject(getRootContext());
        List<Object[]> objects = execute.toList().blockingGet();
        Assert.assertEquals("[[1]]",print(objects));
    }

    @Test
    public void test3() {
        ExecuteCompiler relBuilder = new ExecuteCompiler();
        relBuilder.values(SchemaBuilder.ofArrowType(ArrowTypes.INT32_TYPE, ArrowTypes.STRING_TYPE).toArrow(), Arrays.asList(new Object[]{1, "2"}, new Object[]{3, "4"}));
        relBuilder.project(relBuilder.column(0));
        relBuilder.agg(Collections.emptyList(),Arrays.asList(new CountAggregateFunction()));
        PhysicalPlan build = relBuilder.build();
        OutputLinq4jPhysicalPlan outputLinq4jPhysicalPlan = new OutputLinq4jPhysicalPlan(build);
        Observable<Object[]> execute = outputLinq4jPhysicalPlan.executeToObject(getRootContext());
        List<Object[]> objects = execute.toList().blockingGet();
        Assert.assertEquals("[[2]]",print(objects));
    }
    @Test
    public void testCountStar() {
        ExecuteCompiler relBuilder = new ExecuteCompiler();
        relBuilder.values(SchemaBuilder.ofArrowType(ArrowTypes.INT32_TYPE, ArrowTypes.STRING_TYPE).toArrow(), Arrays.asList(new Object[]{1, "2"}, new Object[]{3, "4"}));
        relBuilder.project(relBuilder.column(0));
        relBuilder.agg(Collections.emptyList(),Arrays.asList(relBuilder.count()));
        PhysicalPlan build = relBuilder.build();
        OutputLinq4jPhysicalPlan outputLinq4jPhysicalPlan = new OutputLinq4jPhysicalPlan(build);
        Observable<Object[]> execute = outputLinq4jPhysicalPlan.executeToObject(getRootContext());
        List<Object[]> objects = execute.toList().blockingGet();
        Assert.assertEquals("[[2]]",print(objects));
    }
    @Test
    public void testCountColumn() {
        {
            ExecuteCompiler relBuilder = new ExecuteCompiler();
            relBuilder.values(SchemaBuilder.ofArrowType(ArrowTypes.INT32_TYPE, ArrowTypes.STRING_TYPE).toArrow(), Arrays.asList(new Object[]{1, "2"}, new Object[]{3, "4"}));
            relBuilder.project(relBuilder.column(0));
            relBuilder.agg(Collections.emptyList(),Arrays.asList( relBuilder.count(0)));
            PhysicalPlan build = relBuilder.build();
            OutputLinq4jPhysicalPlan outputLinq4jPhysicalPlan = new OutputLinq4jPhysicalPlan(build);
            Observable<Object[]> execute = outputLinq4jPhysicalPlan.executeToObject(getRootContext());
            List<Object[]> objects = execute.toList().blockingGet();
            Assert.assertEquals("[[2]]",print(objects));
    }
        {
            ExecuteCompiler relBuilder = new ExecuteCompiler();
            relBuilder.values(SchemaBuilder.ofArrowType(ArrowTypes.INT32_TYPE, ArrowTypes.STRING_TYPE).toArrow(),
                    Arrays.asList(
                            new Object[]{1, "2"},
                            new Object[]{3, "4"},
                            new Object[]{null, "4"}
                    ));
            relBuilder.project(relBuilder.column(0));
            relBuilder.agg(Collections.emptyList(),Arrays.asList( relBuilder.count(0)));
            PhysicalPlan build = relBuilder.build();
            OutputLinq4jPhysicalPlan outputLinq4jPhysicalPlan = new OutputLinq4jPhysicalPlan(build);
            Observable<Object[]> execute = outputLinq4jPhysicalPlan.executeToObject(getRootContext());
            List<Object[]> objects = execute.toList().blockingGet();
            Assert.assertEquals("[[2]]",print(objects));
        }
    }
    @Test
    public void testCountColumn2() {

        {
            ExecuteCompiler relBuilder = new ExecuteCompiler();
            relBuilder.values(SchemaBuilder.ofArrowType(ArrowTypes.INT32_TYPE, ArrowTypes.STRING_TYPE).toArrow(),
                    Arrays.asList(
                            new Object[]{1, "2"},
                            new Object[]{3, "4"},
                            new Object[]{null, "4"}
                    ));
            relBuilder.project(relBuilder.column(0));
            relBuilder.agg(Collections.emptyList(),Arrays.asList( relBuilder.count(0)));
            PhysicalPlan build = relBuilder.build();
            OutputLinq4jPhysicalPlan outputLinq4jPhysicalPlan = new OutputLinq4jPhysicalPlan(build);
            Observable<Object[]> execute = outputLinq4jPhysicalPlan.executeToObject(getRootContext());
            List<Object[]> objects = execute.toList().blockingGet();
            Assert.assertEquals("[[2]]",print(objects));
        }
    }
    @Test
    public void testSum() {
        ExecuteCompiler relBuilder = new ExecuteCompiler();
        relBuilder.values(SchemaBuilder.ofArrowType(ArrowTypes.INT32_TYPE, ArrowTypes.STRING_TYPE).toArrow(), Arrays.asList(new Object[]{1, "2"}, new Object[]{3, "4"}));
        relBuilder.project(relBuilder.column(0));
        relBuilder.agg(Collections.emptyList(),Arrays.asList( relBuilder.sum(0)));
        PhysicalPlan build = relBuilder.build();
        OutputLinq4jPhysicalPlan outputLinq4jPhysicalPlan = new OutputLinq4jPhysicalPlan(build);
        Observable<Object[]> execute = outputLinq4jPhysicalPlan.executeToObject(getRootContext());
        List<Object[]> objects = execute.toList().blockingGet();
        Assert.assertEquals("[[4]]",print(objects));
    }
    @Test
    public void testAvg() {
        ExecuteCompiler relBuilder = new ExecuteCompiler();
        relBuilder.values(SchemaBuilder.ofArrowType(ArrowTypes.INT32_TYPE, ArrowTypes.STRING_TYPE).toArrow(), Arrays.asList(new Object[]{1, "2"}, new Object[]{3, "4"}));
        relBuilder.project(relBuilder.column(0));
        relBuilder.agg(Collections.emptyList(),Arrays.asList( relBuilder.avg(0)));
        PhysicalPlan build = relBuilder.build();
        OutputLinq4jPhysicalPlan outputLinq4jPhysicalPlan = new OutputLinq4jPhysicalPlan(build);
        Observable<Object[]> execute = outputLinq4jPhysicalPlan.executeToObject(getRootContext());
        List<Object[]> objects = execute.toList().blockingGet();
        Assert.assertEquals("[[2.0]]",print(objects));
    }
    @Test
    public void testMin() {
        ExecuteCompiler relBuilder = new ExecuteCompiler();
        relBuilder.values(SchemaBuilder.ofArrowType(ArrowTypes.INT32_TYPE, ArrowTypes.STRING_TYPE).toArrow(), Arrays.asList(new Object[]{1, "2"}, new Object[]{3, "4"}));
        relBuilder.project(relBuilder.column(0));
        relBuilder.agg(Collections.emptyList(),Arrays.asList( relBuilder.min(0)));
        PhysicalPlan build = relBuilder.build();
        OutputLinq4jPhysicalPlan outputLinq4jPhysicalPlan = new OutputLinq4jPhysicalPlan(build);
        Observable<Object[]> execute = outputLinq4jPhysicalPlan.executeToObject(getRootContext());
        List<Object[]> objects = execute.toList().blockingGet();
        Assert.assertEquals("[[1]]",print(objects));
    }
    @Test
    public void testMax() {
        ExecuteCompiler relBuilder = new ExecuteCompiler();
        relBuilder.values(SchemaBuilder.ofArrowType(ArrowTypes.INT32_TYPE, ArrowTypes.STRING_TYPE).toArrow(), Arrays.asList(new Object[]{1, "2"}, new Object[]{3, "4"}));
        relBuilder.project(relBuilder.column(0));
        relBuilder.agg(Collections.emptyList(),Arrays.asList( relBuilder.min(0)));
        PhysicalPlan build = relBuilder.build();
        OutputLinq4jPhysicalPlan outputLinq4jPhysicalPlan = new OutputLinq4jPhysicalPlan(build);
        Observable<Object[]> execute = outputLinq4jPhysicalPlan.executeToObject(getRootContext());
        List<Object[]> objects = execute.toList().blockingGet();
        Assert.assertEquals("[[1]]",print(objects));
    }
    @Test
    public void testCountDistinct() {
        ExecuteCompiler relBuilder = new ExecuteCompiler();
        relBuilder.values(SchemaBuilder.ofArrowType(ArrowTypes.INT32_TYPE, ArrowTypes.STRING_TYPE).toArrow(),
                Arrays.asList(new Object[]{1, "2"},
                        new Object[]{3, "4"},
                        new Object[]{1, "2"}
                ));
        relBuilder.project(relBuilder.column(0));
        relBuilder.agg(Collections.emptyList(),Arrays.asList( relBuilder.countDistinct(0)));
        PhysicalPlan build = relBuilder.build();
        OutputLinq4jPhysicalPlan outputLinq4jPhysicalPlan = new OutputLinq4jPhysicalPlan(build);
        Observable<Object[]> execute = outputLinq4jPhysicalPlan.executeToObject(getRootContext());
        List<Object[]> objects = execute.toList().blockingGet();
        Assert.assertEquals("[[2]]",print(objects));
    }
    @Test
    public void testDistinct() {
        ExecuteCompiler relBuilder = new ExecuteCompiler();
        relBuilder.values(SchemaBuilder.ofArrowType(ArrowTypes.INT32_TYPE, ArrowTypes.STRING_TYPE).toArrow(),
                Arrays.asList(new Object[]{1, "2"},
                        new Object[]{3, "4"},
                        new Object[]{1, "2"}
                        ));
        relBuilder.project(relBuilder.column(0));
        relBuilder.distinct(ExecuteCompiler.AggImpl.HASH);
        PhysicalPlan build = relBuilder.build();
        OutputLinq4jPhysicalPlan outputLinq4jPhysicalPlan = new OutputLinq4jPhysicalPlan(build);
        Observable<Object[]> execute = outputLinq4jPhysicalPlan.executeToObject(getRootContext());
        List<Object[]> objects = execute.toList().blockingGet();
        Assert.assertEquals("[[1], [3]]",print(objects));
    }
    @Test
    public void testGroupByKey() {
        ExecuteCompiler relBuilder = new ExecuteCompiler();
        relBuilder.values(SchemaBuilder.ofArrowType(ArrowTypes.INT32_TYPE, ArrowTypes.STRING_TYPE).toArrow(),
                Arrays.asList(new Object[]{1, "2"},
                        new Object[]{3, "4"},
                        new Object[]{1, "2"}
                ));
        relBuilder.project(relBuilder.column(0));
        relBuilder.agg(ExecuteCompiler.AggImpl.HASH, GroupKeys.of(new int[]{0}));
        PhysicalPlan build = relBuilder.build();
        OutputLinq4jPhysicalPlan outputLinq4jPhysicalPlan = new OutputLinq4jPhysicalPlan(build);
        Observable<Object[]> execute = outputLinq4jPhysicalPlan.executeToObject(getRootContext());
        List<Object[]> objects = execute.toList().blockingGet();
        Assert.assertEquals("[[1], [3]]",print(objects));
    }

    @NotNull
    private RootContext getRootContext() {
        return rootContext;
    }

    private String print(List<Object[]> objects) {
        return objects.stream().map(i -> Arrays.toString(i)).collect(Collectors.toList()).toString();
    }

    @Test
    public void testGroupByKeyWithAgg() {
        ExecuteCompiler relBuilder = new ExecuteCompiler();
        relBuilder.values(SchemaBuilder.ofArrowType(ArrowTypes.INT32_TYPE, ArrowTypes.STRING_TYPE).toArrow(),
                Arrays.asList(new Object[]{1, "2"},
                        new Object[]{3, "4"},
                        new Object[]{1, "2"}
                ));
        relBuilder.project(relBuilder.column(0));
        relBuilder.agg(ExecuteCompiler.AggImpl.HASH,Arrays.asList( GroupKeys.of(new int[]{0})),Arrays.asList(relBuilder.sum(0)));
        PhysicalPlan build = relBuilder.build();
        OutputLinq4jPhysicalPlan outputLinq4jPhysicalPlan = new OutputLinq4jPhysicalPlan(build);
        Observable<Object[]> execute = outputLinq4jPhysicalPlan.executeToObject(getRootContext());
        List<Object[]> objects = execute.toList().blockingGet();
        Assert.assertEquals("[[2], [3]]",print(objects));
    }
    @Test
    public void testLimit() {
        ExecuteCompiler relBuilder = new ExecuteCompiler();
        relBuilder.values(SchemaBuilder.ofArrowType(ArrowTypes.INT32_TYPE, ArrowTypes.STRING_TYPE).toArrow(),
                Arrays.asList(new Object[]{1, "2"},
                        new Object[]{3, "4"},
                        new Object[]{1, "2"}
                ));
        relBuilder.limit(new IntBindVariable(1,false),new IntBindVariable(1,false));
        PhysicalPlan build = relBuilder.build();
        OutputLinq4jPhysicalPlan outputLinq4jPhysicalPlan = new OutputLinq4jPhysicalPlan(build);
        Observable<Object[]> execute = outputLinq4jPhysicalPlan.executeToObject(getRootContext());
        List<Object[]> objects = execute.toList().blockingGet();
        Assert.assertEquals("[[3, 4]]",print(objects));
    }

    @Test
    public void testSort() {
        ExecuteCompiler relBuilder = new ExecuteCompiler();
        relBuilder.values(SchemaBuilder.ofArrowType(ArrowTypes.INT32_TYPE, ArrowTypes.STRING_TYPE).toArrow(),
                Arrays.asList(new Object[]{1, "2"},
                        new Object[]{3, "4"},
                        new Object[]{1, "2"}
                ));
        relBuilder.sort(Arrays.asList(PhysicalSortProperty.of(0, SortOptions.defaultValue(),InnerType.INT32_TYPE),PhysicalSortProperty.of(1,SortOptions.defaultValue(),InnerType.INT32_TYPE)));
        PhysicalPlan build = relBuilder.build();
        OutputLinq4jPhysicalPlan outputLinq4jPhysicalPlan = new OutputLinq4jPhysicalPlan(build);
        Observable<Object[]> execute = outputLinq4jPhysicalPlan.executeToObject(getRootContext());
        List<Object[]> objects = execute.toList().blockingGet();
        Assert.assertEquals("[[1, 2], [1, 2], [3, 4]]",print(objects));
    }

    @Test
    public void testNJoin() {
        ExecuteCompiler relBuilder = new ExecuteCompiler();
        relBuilder.values(SchemaBuilder.ofArrowType(ArrowTypes.INT32_TYPE, ArrowTypes.STRING_TYPE).toArrow(),
                Arrays.asList(new Object[]{1, "2"},
                        new Object[]{2, "4"},
                        new Object[]{3, "2"}
                ));
        relBuilder.values(SchemaBuilder.ofArrowType(ArrowTypes.INT32_TYPE, ArrowTypes.STRING_TYPE).toArrow(),
                Arrays.asList(new Object[]{1, "22"},
                        new Object[]{2, "44"},
                        new Object[]{3, "22"}
                ));
        relBuilder.startJoin();
        relBuilder.crossJoin(JoinType.INNER, ExecuteCompiler.JoinImpl.NL,relBuilder.call("=",relBuilder.column(0),relBuilder.column(2)));
        PhysicalPlan build = relBuilder.build();
        OutputLinq4jPhysicalPlan outputLinq4jPhysicalPlan = new OutputLinq4jPhysicalPlan(build);
        Observable<Object[]> execute = outputLinq4jPhysicalPlan.executeToObject(getRootContext());
        List<Object[]> objects = execute.toList().blockingGet();
        Assert.assertEquals("[[1, 2, 1, 22], [2, 4, 2, 44], [3, 2, 3, 22]]",print(objects));
    }
//
    @Test
    public void testCorJoin() {
        ExecuteCompiler relBuilder = new ExecuteCompiler();
        relBuilder.values(SchemaBuilder.ofArrowType(ArrowTypes.INT32_TYPE, ArrowTypes.STRING_TYPE).toArrow(),
                Arrays.asList(new Object[]{1, "2"},
                        new Object[]{2, "4"},
                        new Object[]{3, "2"}
                ));
        relBuilder.values(SchemaBuilder.ofArrowType(ArrowTypes.INT32_TYPE, ArrowTypes.STRING_TYPE).toArrow(),
                Arrays.asList(new Object[]{1, "22"},
                        new Object[]{2, "44"},
                        new Object[]{3, "22"}
                ));
        VariableParameterFunction variableParameterFunction = relBuilder.newCorVariable(InnerType.INT32_TYPE);
        relBuilder.filter(relBuilder.call("=",relBuilder.column(0),variableParameterFunction));
        relBuilder.correlate(JoinType.INNER, Collections.singletonMap(0,Arrays.asList(variableParameterFunction)));
        PhysicalPlan build = relBuilder.build();
        OutputLinq4jPhysicalPlan outputLinq4jPhysicalPlan = new OutputLinq4jPhysicalPlan(build);
        Observable<Object[]> execute = outputLinq4jPhysicalPlan.executeToObject(getRootContext());
        List<Object[]> objects = execute.toList().blockingGet();
        Assert.assertEquals("[[1, 2], [2, 4]]",print(objects));
    }

    @Test
    public void testIndexParam() {
        ExecuteCompiler relBuilder = new ExecuteCompiler();
        relBuilder.values(SchemaBuilder.ofArrowType(ArrowTypes.INT32_TYPE, ArrowTypes.STRING_TYPE).toArrow(),
                Arrays.asList(new Object[]{1, "2"},
                        new Object[]{2, "4"},
                        new Object[]{3, "2"}
                ));
        relBuilder.values(SchemaBuilder.ofArrowType(ArrowTypes.INT32_TYPE, ArrowTypes.STRING_TYPE).toArrow(),
                Arrays.asList(new Object[]{1, "22"},
                        new Object[]{2, "44"},
                        new Object[]{3, "22"}
                ));

        ArrayList<IndexedParameterLinkFunction> indexedParameterLinkFunctions = new ArrayList<>();
        IndexedParameterLinkFunction variableParameterFunction = relBuilder.newIndexVariable(0,InnerType.INT32_TYPE);
        indexedParameterLinkFunctions.add(variableParameterFunction);
        relBuilder.filter(relBuilder.call("=",relBuilder.column(0),variableParameterFunction));
        PhysicalPlan build = relBuilder.build();
        OutputLinq4jPhysicalPlan outputLinq4jPhysicalPlan = new OutputLinq4jPhysicalPlan(build);
        IntBindVariable intBindVariable = (IntBindVariable) variableParameterFunction.getBase();
        intBindVariable.value = 1;
        Observable<Object[]> execute = outputLinq4jPhysicalPlan.executeToObject(getRootContext());
        List<Object[]> objects = execute.toList().blockingGet();
        Assert.assertEquals("[[1, 22]]",print(objects));
    }
}