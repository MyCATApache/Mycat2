package io.mycat.calcite;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class MetadataManagerTest {
    static Map<BackEndTableInfo, String> route(String currentSchema, String sql){
       return MetadataManager.INSATNCE.rewriteUpdateSQL(currentSchema,sql);
    }
    @Test
    public void test() {
        Map<BackEndTableInfo, String> rs = route("TESTDB","select * from TESTDB.travelrecord where id between 1 and 999" );
        System.out.println(rs);
    }
}