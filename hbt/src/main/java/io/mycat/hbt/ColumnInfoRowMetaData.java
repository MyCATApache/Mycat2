package io.mycat.hbt;

import io.mycat.api.collector.AbstractObjectRowIterator;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.hbt.ast.query.FieldType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.jetbrains.annotations.NotNull;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;


public class ColumnInfoRowMetaData extends HbtRowMetaData {
    public final static ColumnInfoRowMetaData INSTANCE = new ColumnInfoRowMetaData();
    private static    ArrayList<BiFunction<MycatRowMetaData, Integer, Object>> GETTERS  ;


private ColumnInfoRowMetaData(){
    this(init());
}

    @NotNull
    private static List<FieldType> init() {
        GETTERS  =new ArrayList<>() ;;
        ArrayList<FieldType> objects = new ArrayList<>();
        String booleanName = SqlTypeName.BOOLEAN.getName();
        String stringName = SqlTypeName.VARCHAR.getName();
        String integerName = SqlTypeName.INTEGER.getName();
        addColumnInfo(objects, "columnName", stringName, MycatRowMetaData::getColumnName);
        addColumnInfo(objects, "columnType", stringName, (mycatRowMetaData, column) -> JDBCType.valueOf( mycatRowMetaData.getColumnType(column)).getName());

//        addColumnInfo(objects, "columnTypeName", stringName);
        addColumnInfo(objects, "isNullable", booleanName, MycatRowMetaData::isNullable);
        addColumnInfo(objects, "precision", integerName, MycatRowMetaData::getPrecision);
        addColumnInfo(objects, "scale", integerName, MycatRowMetaData::getScale);

        addColumnInfo(objects, "isAutoIncrement", booleanName, MycatRowMetaData::isAutoIncrement);
        addColumnInfo(objects, "isCaseSensitive", booleanName, MycatRowMetaData::isCaseSensitive);
//        addColumnInfo(objects, "isSearchable",booleanName);
//        addColumnInfo(objects, "isCurrency", booleanName);
        addColumnInfo(objects, "isSigned", booleanName, MycatRowMetaData::isSigned);
        addColumnInfo(objects, "columnDisplaySize", integerName, MycatRowMetaData::getColumnDisplaySize);
        addColumnInfo(objects, "columnLabel", stringName, MycatRowMetaData::getColumnLabel);
//        addColumnInfo(objects, "schemaName", stringName);
        addColumnInfo(objects, "tableName", stringName, MycatRowMetaData::getTableName);
//        addColumnInfo(objects, "catalogName", stringName);
//        addColumnInfo(objects, "isReadOnly", booleanName);
//        addColumnInfo(objects, "isWritable", booleanName);
//        addColumnInfo(objects, "isDefinitelyWritable",booleanName);
//        addColumnInfo(objects, "columnClassName", stringName);
        return objects;
    }

    private  static void addColumnInfo(ArrayList<FieldType> objects, String isAutoIncrement, String varchar, BiFunction<MycatRowMetaData, Integer, Object> getter) {
        objects.add(FieldType.builder().id(isAutoIncrement).type(varchar).build());
        GETTERS.add(getter);
    }

    private ColumnInfoRowMetaData(List<FieldType> fieldTypeList) {
        super(fieldTypeList);
    }
    public static RowBaseIterator convertToRowIterator(MycatRowMetaData mycatRowMetaData) {
        return new AbstractObjectRowIterator() {
            int index = 1;

            @Override
            public MycatRowMetaData getMetaData() {
                return ColumnInfoRowMetaData.INSTANCE;
            }

            @Override
            public boolean next() {
                boolean b = index <= mycatRowMetaData.getColumnCount();

                if (b) {
                    Object[] objects = new Object[GETTERS.size()];
                    for (int i = 0; i < objects.length; i++) {
                        BiFunction<MycatRowMetaData, Integer, Object> mycatRowMetaDataIntegerObjectBiFunction = GETTERS.get(i);
                        objects[i] = mycatRowMetaDataIntegerObjectBiFunction.apply(mycatRowMetaData, index);
                    }
                    this.currentRow = objects;
                    index++;
                    return true;
                } else {
                    return false;
                }
            }

            @Override
            public void close() {

            }
        };
    }

}