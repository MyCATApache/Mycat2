package io.mycat.calcite;

import com.google.common.collect.ImmutableList;
import io.mycat.beans.mycat.MycatDataType;
import io.mycat.beans.mycat.MycatField;
import io.mycat.beans.mycat.MycatRelDataType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import java.sql.JDBCType;
import java.util.List;

public class MycatRelDataTypeUtil {
    public static MycatRelDataType getMycatRelDataType(RelDataType relDataType){
        List<RelDataTypeField> fieldList = relDataType.getFieldList();
        ImmutableList.Builder<MycatField> builder = ImmutableList.builder();
        for (RelDataTypeField relDataTypeField : fieldList) {
            SqlTypeName sqlTypeName = relDataTypeField.getType().getSqlTypeName();
            int jdbcOrdinal = sqlTypeName.getJdbcOrdinal();
            MycatDataType mycatDataType = MycatDataType.fromJdbc(JDBCType.valueOf(jdbcOrdinal), true);
            MycatField mycatField = MycatField.of(relDataTypeField.getName(), mycatDataType, relDataTypeField.getType().isNullable());
            builder.add(mycatField);
        }
        return MycatRelDataType.of(builder.build());
    }
}
