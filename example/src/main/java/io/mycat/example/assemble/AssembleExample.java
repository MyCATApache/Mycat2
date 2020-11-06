package io.mycat.example.assemble;

import io.mycat.example.ExampleObject;
import io.mycat.example.TestUtil;
import io.mycat.example.sharding.ShardingExample;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;

public class AssembleExample{

    @Test
    public void testWrapper() throws Exception {
        Connection mySQLConnection = TestUtil.getMySQLConnection();
        System.out.println();
    }

}
