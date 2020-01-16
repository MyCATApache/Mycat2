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
        stuInfo[0] = new BackEndTableInfo("127.0.0.1", "db1", "travelrecord");
        stuInfo[1] = new BackEndTableInfo("127.0.0.1", "db1", "travelrecord2");

        BackEndTableInfo[] scoInfo = new BackEndTableInfo[2];

        scoInfo[0] = new BackEndTableInfo("127.0.0.1", "db1", "t1");
        scoInfo[1] = new BackEndTableInfo("127.0.0.1", "db1", "t2");
        testSchema.add("TRAVELRECORD", new JdbcTable("TEST", "TRAVELRECORD", stuInfo));
        testSchema.add("T", new JdbcTable("TEST", "T", scoInfo));
        return  metaData;
    }
}
