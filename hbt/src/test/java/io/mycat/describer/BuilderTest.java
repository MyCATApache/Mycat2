package io.mycat.describer;

import io.mycat.describer.literal.IdLiteral;
import io.mycat.rsqlBuilder.DotCallResolver;
import io.mycat.rsqlBuilder.NameBuilder;
import io.mycat.rsqlBuilder.schema.SchemaMatcher;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class BuilderTest {

    @Test
    public void id() throws IOException {
        Describer describer = new Describer("treavelrecord");
        ParseNode primary = describer.primary();
        NameBuilder rexBuilder = new NameBuilder();
        primary.accept(rexBuilder);
        Assert.assertEquals(new IdLiteral("treavelrecord"), rexBuilder.getStack());
    }

    @Test
    public void schema() throws IOException {
        Describer describer = new Describer("db1");
        ParseNode primary = describer.expression();
        SchemaMatcher schemaMatcher = new SchemaMatcher();
        schemaMatcher.addSchema("db1", null, null);
        NameBuilder rexBuilder = new NameBuilder(schemaMatcher);
        primary.accept(rexBuilder);
        Assert.assertEquals("SchemaObject{db1}", Objects.toString(rexBuilder.getStack()));
    }

    @Test
    public void table() throws IOException {
        Describer describer = new Describer("db1.travelrecord");
        ParseNode primary = describer.expression();
        SchemaMatcher schemaMatcher = new SchemaMatcher();
        schemaMatcher.addSchema("db1", "travelrecord", null);
        NameBuilder rexBuilder = new NameBuilder(schemaMatcher);
        primary.accept(rexBuilder);
        Assert.assertEquals("TableObject{db1.travelrecord}", Objects.toString(rexBuilder.getStack()));
    }

    @Test
    public void column() throws IOException {
        Describer describer = new Describer("db1.travelrecord.id");
        ParseNode primary = describer.expression();
        SchemaMatcher schemaMatcher = new SchemaMatcher();
        schemaMatcher.addSchema("db1", "travelrecord", "id");
        NameBuilder rexBuilder = new NameBuilder(schemaMatcher);
        primary.accept(rexBuilder);
        Assert.assertEquals("ColumnObject{db1.travelrecord.id}", Objects.toString(rexBuilder.getStack()));
    }


    @Test
    public void dotCallResolver() throws IOException {
        Describer describer = new Describer("from().map() ");
        ParseNode primary = describer.expression();
        DotCallResolver callResolver = new DotCallResolver();
        primary.accept(callResolver);
        Assert.assertTrue("map(from())".equalsIgnoreCase(Objects.toString(callResolver.getStack())));
    }

    @Test
    public void dotCallResolver2() throws IOException {
        Describer describer = new Describer("from(1).map(2) ");
        ParseNode primary = describer.expression();
        DotCallResolver callResolver = new DotCallResolver();
        primary.accept(callResolver);
        Assert.assertTrue("map(from(1),2)".equalsIgnoreCase(Objects.toString(callResolver.getStack())));
    }

    @Test
    public void project() throws IOException {
        Describer describer = new Describer("(let t = db1.travelrecord).project(t.id as id,t.id as id2)");
        SchemaMatcher schemaMatcher = new SchemaMatcher();
        schemaMatcher.addSchema("db1", "travelrecord", "id");

        //////////////////////
        NameBuilder rexBuilder = getRexBuilder(describer, schemaMatcher);

        Assert.assertEquals("project((TableObject{db1.travelrecord}),as(ColumnObject{db1.travelrecord.id},id),as(ColumnObject{db1.travelrecord.id},id2))", Objects.toString(rexBuilder.getStack()));
    }

    public static NameBuilder getRexBuilder(Describer describer, SchemaMatcher schemaMatcher) {
        ParseNode primary = describer.expression();
        Map<String, ParseNode> variables = describer.getVariables();

        variables.entrySet().forEach(stringNodeEntry -> stringNodeEntry.setValue(processDotCall(stringNodeEntry.getValue())));
        primary = processDotCall(primary);

        variables.entrySet().forEach(c -> {
            NameBuilder rexBuilder = new NameBuilder(schemaMatcher, variables);
            c.getValue().accept(rexBuilder);
            c.setValue(rexBuilder.getStack());
        });
        NameBuilder rexBuilder = new NameBuilder(schemaMatcher, variables);
        primary.accept(rexBuilder);
        return rexBuilder;
    }

    private static ParseNode processDotCall(ParseNode primary) {
        DotCallResolver callResolver = new DotCallResolver();
        primary.accept(callResolver);
        primary = callResolver.getStack();
        return primary;
    }

    @Test
    public void join() throws IOException {
        Describer describer = new Describer("(let j =  " +
                "join((let t1 = db1.travelrecord),(let t2 = db1.address),(t1.id = t2.id)))" +
                ".project(j.id as id)");
        SchemaMatcher schemaMatcher = new SchemaMatcher();
        schemaMatcher.addSchema("db1", "travelrecord", "id");
        schemaMatcher.addSchema("db1", "address", "id");
        //////////////////////
        NameBuilder rexBuilder = getRexBuilder(describer, schemaMatcher);

        Assert.assertEquals("project((join((TableObject{db1.travelrecord}),(TableObject{db1.address}),(eq(ColumnObject{db1.travelrecord.id},ColumnObject{db1.address.id})))),as(j.id,id))", Objects.toString(rexBuilder.getStack()));
    }

    @Test
    public void apply() throws IOException {
        Describer describer = new Describer("(let t = db1.travelrecord).all((let a =  db1.address).project(a.id).all(a.id=t.id)).project(t.id)");
        SchemaMatcher schemaMatcher = new SchemaMatcher();
        schemaMatcher.addSchema("db1", "travelrecord", "id");
        schemaMatcher.addSchema("db1", "address", "id");
        NameBuilder rexBuilder = getRexBuilder(describer, schemaMatcher);
        Assert.assertEquals("project(all((TableObject{db1.travelrecord}),all(project((TableObject{db1.address}),ColumnObject{db1.address.id}),eq(ColumnObject{db1.address.id},ColumnObject{db1.travelrecord.id}))),ColumnObject{db1.travelrecord.id})", Objects.toString(rexBuilder.getStack()));
    }
    @Test
    public void dotCallResolver3() throws IOException {
        Describer describer = new Describer("join (travelrecord as t,address as a, t.id = a.id)\n" +
                ".filter (t.id = 1 or a.id = 2)\n" +
                ".map(t.id,t.user_id)");
//        Describer describer = new Describer("(db1.travelrecord as t).filter(true).map(2) ");
        ParseNode primary = describer.expression();
        SchemaMatcher schemaMatcher = new SchemaMatcher();
        schemaMatcher.addSchema("db1", "travelrecord", "id");
        primary = processDotCall(primary);
        System.out.println(primary);
        NameBuilder rexBuilder = new NameBuilder(schemaMatcher);
        primary.accept(rexBuilder);
        Object stack = rexBuilder.getStack();
        System.out.println(stack);
    }
}