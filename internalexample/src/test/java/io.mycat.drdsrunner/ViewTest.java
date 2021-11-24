package io.mycat.drdsrunner;

import org.junit.Assert;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;

import static io.mycat.drdsrunner.DrdsTest.parse;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class ViewTest {

    @Test
    public void testDistView() throws Exception {
        Explain explain1 = parse("select * from db1.testView");

        Assert.assertEquals("[Each(targetName=prototype, sql=SELECT `normal`.`id` FROM db1.normal AS `normal`)]",explain1.specificSql().toString());

        Explain explain2 = parse("select 1,rename_id from db1.testView2");

        Assert.assertEquals("[Each(targetName=prototype, sql=SELECT ? AS `?`, `normal`.`id` AS `rename_id` FROM db1.normal AS `normal`)]",explain2.specificSql().toString());


        Explain explain3 = parse("select 1,rename_id from db1.testView2");

        Assert.assertEquals("[Each(targetName=prototype, sql=SELECT ? AS `?`, `normal`.`id` AS `rename_id` FROM db1.normal AS `normal`)]",explain3.specificSql().toString());



    }
}
