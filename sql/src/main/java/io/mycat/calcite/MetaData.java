package io.mycat.calcite;

import io.mycat.calcite.table.JdbcTable;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;

public class MetaData {
    private static MetaData metaData;
    private static boolean isOK;

    private MetaData() {}
    public CalciteSchema schema;

    public static MetaData getMetaData(CalciteConnection connection) throws Exception {
        if (isOK) {
        //    return metaData;
        }
        metaData = new MetaData();
        metaData.schema = CalciteSchema.createRootSchema(true);

        //SchemaPlus testSchema = metaData.schema.add("test", new AbstractSchema()).plus();
        SchemaPlus testSchema = connection.getRootSchema().add("TEST", new AbstractSchema());
        BackEndTableInfo[] stuInfo = new BackEndTableInfo[2];
        stuInfo[0] = new BackEndTableInfo("127.0.0.1", "test", "STUDENT");
        stuInfo[1] = new BackEndTableInfo("127.0.0.1", "test", "STUDENT1");

        BackEndTableInfo[] scoInfo = new BackEndTableInfo[2];

        scoInfo[0] = new BackEndTableInfo("127.0.0.1", "test", "SCORE");
        scoInfo[1] = new BackEndTableInfo("127.0.0.1", "test", "SCORE1");
        testSchema.add("STUDENT", new JdbcTable("test", "STUDENT", stuInfo));
        testSchema.add("SCORE", new JdbcTable("test", "SCORE", scoInfo));
        return  metaData;
    }
}
