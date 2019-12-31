package io.mycat.describer;

import io.mycat.describer.literal.IdLiteral;
import io.mycat.rsqlBuilder.NameBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class BuilderTest2 {

    @Test
    public void id() throws IOException {
        Describer describer = new Describer("treavelrecord");
        ParseNode primary = describer.primary();
        NameBuilder rexBuilder = new NameBuilder();
        primary.accept(rexBuilder);
        Assert.assertEquals(new IdLiteral("treavelrecord"), rexBuilder.getStack());
    }
}